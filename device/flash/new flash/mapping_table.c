#include <flash.h>
#include <string.h>

static int max_tree_height(uint64_t total_entries, uint32_t fanout) {
    if (total_entries <= fanout)
        return 2;
    
    uint64_t capacity = 1;
    int height = 1;
    while (capacity < total_entries) {
        capacity *= fanout;
        height++;
    }
    return height;
}

mapping_table_t * mapping_table_init(const char * name, const uint32_t npage_mappings, const uint32_t block_size, const uint32_t page_cache_capacity, bool create)
{
    if (block_size < 16)
    {
        fprintf(stderr, "Error: fanout must be >= 16\n");
        return NULL;
    }

    // Allocate the mapping table structure
    mapping_table_t *mtable = calloc(1, sizeof(mapping_table_t));
    if (!mtable)
    {
        assert(false);
        return NULL;
    }
    mtable->block_size = block_size;
    mtable->npage_mappings = npage_mappings;

    uint32_t fanout = (block_size - sizeof(flash_block_t)) / sizeof(mapping_table_entry_t); // modify later to after adding metaadata
    mtable->fanout = fanout;

    // Compute padded mappings to next multiple of fanout^levels
    // For simplicity, round up to next multiple of fanout
    uint32_t padded = 1;
    while(npage_mappings > padded)
    {
        padded *= fanout;
    }
    mtable->npage_mappings_padded = padded;

    mtable->nallocated_pages = 0;

    uint32_t lss_cache_capacity = max_tree_height(padded, fanout);

    // decrement one for root
    mtable->page_cache = mtpc_create(page_cache_capacity - 1 - lss_cache_capacity, mtable, NULL);
    if(NULL == mtable->page_cache)
    {
        assert(false);
        free(mtable);
        return NULL;
    }

    // lss cache
    mtpc_t * lss_page_cache = mtpc_create(lss_cache_capacity, mtable, mtable->page_cache);
    if(NULL == mtable->page_cache)
    {
        assert(false);
        free(mtable);
        return NULL;
    }
    
    // TODO: if we are setting up a new storage, then create the lss (which erases the storage)
    // then create the root (now we might need to flush it to acquire an initial offset for it and unify the reopening logic)
    // but for now let's ignore the case where a storage only utilized one segment or less, and never got a chance to either write its root
    // or a segment metadata.
    flash_block_t * root = malloc(block_size);
    if(!root)
    {
        assert(false);
        free(mtable);
        return NULL;
    }
    mtable->root = root;
    root->level = 0;
    root->logical_address = 0;
    root->type = MT_BLOCK;

    int ret = lss_create(&mtable->lss, lss_page_cache, name);
    if(0 != ret)
    {
        assert(false);
        free(mtable);
        return NULL;
    }
    
    if(create)
    {   
        memset(root->data, 0xFF, block_size - sizeof(flash_block_t));    
        int ret = lss_init(&mtable->lss, mtable);
        if(0 != ret)
        {
            assert(false);
            free(mtable);
            return NULL;
        }
    }
    else
    {
        uint64_t root_offset = FLASH_INVALID_ADDRESS;
        int ret = lss_load_root_and_seg_meta(&mtable->lss, root, &root_offset);
        if(0 != ret)
        {
            assert(false);
            free(mtable);
            return NULL;
        }
        
        assert(block_size == mtable->lss.curr_segment_metadata.block_size);
        assert(npage_mappings == mtable->lss.curr_segment_metadata.npage_mappings);

        // TODO: finish reocvery, but since currently we are always finishing gracefully, we are sure that 
        // this root contains all the latest information
        
    }

    return mtable;
}


void mapping_table_destroy(mapping_table_t * mtable)
{
    if(!mtable)
    {
        return;
    }

    lss_realloc_evict(mtable);
    mtpc_flush_all(mtable->page_cache, false);
    lss_write(mtable, mtable->root, mtable->block_size, false, &mtable->root_offset);

    mtpc_destroy(mtable->page_cache);
    mtpc_destroy(mtable->lss.page_cache);

    lss_destroy(&mtable->lss);
    free(mtable->root);
    free(mtable);
}

/*
    Given a logical address, return a pointer to the physical address of the page
    Now this function will search through the mapping table tree, also since we know beforehand the 
    structure of the tree, there is no need to physically construct the entire tree, instead, we only
    iterate through the tree, and if we found that a child has an invalid address, then we know that
    this branch wasn't explored before and the page comes into existence

    New mapping table that comes into existence will only exist in cache an
*/
int mapping_table_update_physical_address(mapping_table_t * mtable, const uint64_t logical_address, const uint64_t physical_address, const bool clean_tail)
{
    /*
        So, when it comes to the alogirthm we can use one of two approaches
        - Since we are already utilizing a fixed tree structure, then the ranges held by each
        page is fixed, so, we can simply first go through the cache to find the page that refines our query range the most
        and then work with it, and we can even find our page directly, but the down side of this approach, is that the path of
        that entry isn't in-memory, so when we want to flush that page and update its path, we might have to load it first in-memory
        which if the path have to be first loaded in cache, this might result in swapping another page, which results in needing to load
        its path as well, and so on and so forth and everntually we get stuck in this inifinite loop, one way to solve this problem, is to
        either accept the increase in-memory, or to excplicilty leave a nubmer of pages unused in cache to accomadate those extra pages

        - The other solution we can do is to hold onto a path in-memory, and always force flushing from leaf to root, this simplifies things and avoids
        the above problems, but introduces extra complexity, but during runtime it forces us to keep intermediate pages in-memory, which takes up extra entries in cache, which could have been
        used instead for leaf pages holding actual page addresses

        but either way, here it doesn't really change the loopup procedure which basically goes as follows
        - search the cache for the most refined range that contains our target
        - if found the a leaf, then just get the target and return
        - if not then we have to refine more auntil it is in-memory
    */

    if(logical_address >= mtable->npage_mappings)
    {
        return -1;
    }

    flash_block_t * curr_block = NULL; // start from the root
    uint64_t curr_range;
    uint64_t curr_range_start = 0;
    uint32_t fanout = mtable->fanout;
    mtpc_entry_t * cache_entry = NULL;
    while(true)
    {
        // TODO: get target index from curr_block
        // TODO: get page from cache
        // if not in cache swap out a page and swap in the new target
        if(NULL == curr_block)
        {
            cache_entry = mtpc_search(mtable->page_cache, logical_address);
            if(NULL == cache_entry)
            {
                curr_block = mtable->root;
                curr_range_start = 0;
                curr_range = mtable->npage_mappings_padded; // must use padded so we don't worry about fractions or anything
            }
            else
            {
                curr_block = cache_entry->value;
                curr_range_start = cache_entry->start_range;
                curr_range = cache_entry->end_range - cache_entry->start_range;
            }
        }
        assert(NULL != curr_block);
        
        // we are done when the sub_range_size is equal to the fanout
        // since this indicates there is no logner any more possible refinements
        // and thus the address we have is the physical address of the logical page the user wants

        // Compute how many logical entries each child covers
        uint64_t sub_range = curr_range / fanout;
        uint32_t target_index = (logical_address % curr_range) / sub_range;

        // Get entries array from current block
        mapping_table_entry_t * entries = (mapping_table_entry_t *) curr_block->data;

        if(1 == sub_range)
        {
            // we are in a leaf node and the we will modify the entry
            entries[target_index].physical_address = physical_address;
            mtpc_mark_dirty(mtable->page_cache, cache_entry);
            mtpc_unpin(cache_entry);
            return 0;
        }
        else
        {
            // we are in an intermediate node and still need to refine more
            // And So, if we have to refine more, this means that from this point forward
            // the pages are on disk and we have to load them in cache
            uint64_t new_entry_start_range = curr_range_start + (target_index * sub_range);
            curr_range /= fanout;
            curr_range_start = new_entry_start_range;
            mtpc_entry_t * new_cache_entry = NULL;
            if(FLASH_INVALID_ADDRESS == entries[target_index].physical_address)
            {   
                // Allocate a new page
                new_cache_entry = mtpc_insert(mtable->page_cache,
                                                                new_entry_start_range,
                                                                new_entry_start_range + curr_range,
                                                                curr_block->level + 1,
                                                                cache_entry,
                                                                clean_tail);
                assert(NULL != new_cache_entry);
                // zero out new block data only, since inserts only updated the metadata
                memset(new_cache_entry->value->data, 0xFF, mtable->block_size - sizeof(flash_block_t));
            }
            else
            {
                // Read page from storage
                new_cache_entry = mtpc_acquire_from_storage(mtable->page_cache,
                                                            new_entry_start_range, 
                                                            new_entry_start_range + curr_range,
                                                            entries[target_index].physical_address,
                                                            cache_entry,
                                                            clean_tail);
                assert(NULL != new_cache_entry);
                assert((1 + curr_block->level) == new_cache_entry->value->level);
            }

            if(NULL == new_cache_entry)
            {
                assert(false);
                return -1;
            }
            mtpc_unpin(cache_entry);
            curr_block = new_cache_entry->value;
            cache_entry = new_cache_entry;
        }
    }

    return -1;
}

int mapping_table_get_physical_address(mapping_table_t *mtable, uint64_t logical_address, bool clean_tail, uint64_t * physical_address)
{
    if(logical_address >= mtable->npage_mappings)
    {
        return -1;
    }
    
    flash_block_t * curr_block = NULL; // start from the root
    uint64_t curr_range;
    uint64_t curr_range_start;
    uint32_t fanout = mtable->fanout;
    mtpc_entry_t * cache_entry = NULL;
    while(true)
    {
        if(NULL == curr_block)
        {
            // currently, all pages should be in cache, so we must find the page we want directly in cache
            cache_entry = mtpc_search(mtable->page_cache, logical_address);
            if(NULL == cache_entry)
            {
                curr_block = mtable->root;
                curr_range_start = 0;
                curr_range = mtable->npage_mappings_padded; // must use padded so we don't worry about fractions or anything
            }
            else
            {
                curr_block = cache_entry->value;
                curr_range_start = cache_entry->start_range;
                curr_range = cache_entry->end_range - cache_entry->start_range;
            }
        }
        assert(NULL != curr_block);
       
        uint64_t sub_range = curr_range / fanout;
        uint32_t target_index = (logical_address % curr_range) / sub_range;
        mapping_table_entry_t * entries = (mapping_table_entry_t *) curr_block->data;
        if(1 == sub_range)
        {
            // we are in a leaf node and the we will modify the entry
            physical_address[0] = entries[target_index].physical_address;
            mtpc_unpin(cache_entry);
            if(FLASH_INVALID_ADDRESS == physical_address[0])
            {
                return -1;
            }
            return 0;
        }
        else
        {
            if(FLASH_INVALID_ADDRESS == entries[target_index].physical_address)
            {
                mtpc_unpin(cache_entry);
                return -1;
            }
            else
            {
                uint64_t new_entry_start_range = curr_range_start + (target_index * sub_range);
                curr_range /= fanout;
                curr_range_start = new_entry_start_range;
                mtpc_entry_t * new_cache_entry = mtpc_acquire_from_storage(mtable->page_cache,
                                                                        new_entry_start_range, 
                                                                        new_entry_start_range + curr_range,
                                                                        entries[target_index].physical_address,
                                                                        cache_entry,
                                                                        clean_tail);
                if(NULL == new_cache_entry)
                {
                    assert(false);
                    return -1;
                }
                assert((1 + curr_block->level) == new_cache_entry->value->level);
                mtpc_unpin(cache_entry);
                curr_block = new_cache_entry->value;
                cache_entry = new_cache_entry;
            }            
        }
    }
    assert(false);
    return -1;
}

/*
    NO need for now since we always assume graceful shutdowns
    so will never require to replay a log
*/
#if 0
// IMPORTANT: currently since we are checkpointing at the beginning of each segment, we assume that length of the
// log that we need to read starts from the right after the root offset, to the end of the segment containing the root
static int replay_log(mapping_table_t * mtable)
{
    assert(mtable->block_size == mtable->lss.flash->write_granularity_bytes);
    uint32_t segment_size = mtable->lss.flash->segment_size_bytes;
    uint64_t start_offset = mtable->root_offset + mtable->block_size;
    uint64_t end_offset = ((mtable->root_offset / segment_size) + 1) * segment_size;
    uint32_t block_size = mtable->block_size;

    struct log_entry_s
    {
        uint32_t relative_offset; // true offset = start_offset + relative_offset
        flash_block_t hdr;
    };
    

    uint8_t *buf = malloc(block_size);
    if (!buf)
    {
        assert(false);
        return -1;
    }

    for (uint64_t curr_offset = start_offset; curr_offset < end_offset; curr_offset++)
    {
        if (lss_read(mtable, curr_offset, buf, block_size) != 0)
        {
            assert(false);
            continue;
        }

        flash_block_t *hdr = (flash_block_t *)buf;

        // skip erased
        if (*(uint32_t *)hdr == UINT32_MAX)
        {
            continue;
        }
    }
    
}
#endif