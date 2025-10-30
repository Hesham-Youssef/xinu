#include <flash.h>

#include <io_wrapper.h>

#define LSS_OK 0
#define LSS_ERR -1

#ifdef FLASH_DEBUG
static bool is_mt_segment_fully_dead(mapping_table_t *mtable, uint64_t block_address);
static void lss_print_realloc_list(const lss_t *lss);
#endif

static uint64_t lss_get_used_space(const lss_t *lss);
static int lss_clean_tail_block(mapping_table_t * mtable, uint64_t tail_offset);
static bool is_mt_block_alive(mapping_table_t * mtable, uint64_t block_address, flash_block_t * block_header);

static int lss_realloc_insert(mapping_table_t * mtable, flash_block_t * block_header, uint64_t old_offset, uint64_t new_offset);
static bool lss_realloc_lookup(const lss_t *lss, uint64_t old_offset, uint64_t *new_offset_out);
static bool apply_reallocate(mapping_table_t * mtable, flash_block_t * block_header, uint64_t old_block_address, uint64_t new_block_address);

static int lss_write_segment_metadata(lss_t * lss, uint64_t segment_offset);

struct lss_realloc_entry_s {
    uint64_t old_offset;
    uint64_t new_offset;
    flash_block_t block_header;
};

int lss_create(lss_t *lss, mtpc_t *page_cache, const char * name)
{
    if (!lss)
    {
        assert(false);
        return LSS_ERR;
    }
    memset(lss, 0, sizeof(lss_t));

    lss->flash = flash_open(name, FLASH_TOTAL_SIZE);
    if (!lss->flash)
    {
        assert(false);
        return LSS_ERR;
    }

    lss->page_cache = page_cache;

    // Flash must already be opened or created before calling this
    if (!lss->flash)
    {
        assert(false);
        return LSS_ERR;
    }

    lss->realloc_capacity = lss->flash->write_granularity_bytes / sizeof(lss_realloc_entry_t);
    lss->realloc_count = 0;
    lss->realloc_list = malloc(lss->realloc_capacity * sizeof(lss_realloc_entry_t));
    if (!lss->realloc_list)
    {
        assert(false);
        return LSS_ERR;
    }

    return LSS_OK;
}

int lss_init(lss_t *lss, mapping_table_t * mtable)
{
    if (!lss)
    {
        assert(false);
        return LSS_ERR;
    }
    
    if (flash_erase_all(lss->flash) != 0)
    {
        assert(false);
        return LSS_ERR;
    }

    lss->head = 0;
    lss->tail = 0;

    (void)mtable;

    lss_segment_metadata_t * seg_meta = &lss->curr_segment_metadata;
    seg_meta->seq_num = 0;
    seg_meta->type = SEG_META_BLOCK;
        
    seg_meta->contains_checkpoint = true; // TODO: modify later, but for now assume all segments contain a checkpoint
    seg_meta->block_size = mtable->block_size;
    seg_meta->npage_mappings = mtable->npage_mappings;
    seg_meta->tail_offset = lss->tail;

    lss_write_segment_metadata(lss, 0);

    lss_write(mtable, mtable->root, mtable->block_size, false, &mtable->root_offset);

    printf("[LSS] Created new log structure\n");
    return LSS_OK;
}

// This just finds the offset of the latest root on storage
int lss_load_root_and_seg_meta(lss_t *lss, flash_block_t * root_out, uint64_t * root_offset_out)
{
    if (!lss)
    {
        assert(false);
        return LSS_ERR;
    }

    size_t segment_size = lss->flash->segment_size_bytes;
    size_t write_granularity = lss->flash->write_granularity_bytes;
    uint64_t flash_size = flash_get_size(lss->flash); // hmmm, should we assume we already know this information beforehand?
    uint64_t n_segments = flash_size / segment_size;

    uint64_t latest_seq = 0;
    uint64_t latest_seg_offset = FLASH_INVALID_ADDRESS;

    uint8_t *buf = malloc(write_granularity);
    if (!buf)
    {
        assert(false);
        return LSS_ERR;
    }

    // === Step 1: Find the latest checkpoint segment ===
    for (uint64_t i = 0; i < n_segments; i++)
    {
        uint64_t seg_offset = i * segment_size;
        if (flash_read(lss->flash, seg_offset, buf, write_granularity) != 0)
        {
            assert(false);
            continue;
        }

        lss_segment_metadata_t *meta = (lss_segment_metadata_t *)buf;

        // Basic validation
        if (meta->type != SEG_META_BLOCK)
        {
            // this should happen since some blocks may have worn out or were erased so does their metadata
            assert(UINT32_MAX == ((uint32_t *)buf)[0]);
            continue;
        }

        if (meta->contains_checkpoint && meta->seq_num >= latest_seq)
        {
            latest_seq = meta->seq_num;
            latest_seg_offset = seg_offset;
            // HMMMMM, should we keep it in memory and having to copy data a lot of times for no reason
            // or it is better to just track the offsets and then doing an extra read at the end?
            memcpy(&lss->curr_segment_metadata, meta, sizeof(*meta));
        }
    }

    if (latest_seg_offset == FLASH_INVALID_ADDRESS)
    {
        // How to distinguish between old and new storage?
        assert(false);
        printf("[LSS] No checkpointed segment found. Possibly uninitialized storage.\n");
        free(buf);
        return LSS_ERR;
    }

    printf("[LSS] Found checkpoint in segment #%llu (seq=%llu)\n",
           (unsigned long long)(latest_seg_offset / segment_size),
           (unsigned long long)latest_seq);

    // === Step 2: Scan that segment until root is found ===
    uint64_t seg_end = latest_seg_offset + segment_size;
    uint64_t root_offset = FLASH_INVALID_ADDRESS;

    /*
        IMPORTANT: currently for simplicity we assume that the block size is the same as the write_granularity
        but this might not be true in the future
    */
    assert(write_granularity == lss->curr_segment_metadata.block_size);

    for (uint64_t off = latest_seg_offset + write_granularity; off < seg_end; off += write_granularity)
    {
        if (flash_read(lss->flash, off, buf, write_granularity) != 0)
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

        if (hdr->type == MT_BLOCK && hdr->level == 0)
        {
            // There can be multiple roots, since a block can contain a checkpoint and then the user decides to shutdown, so we 
            // flush everything dirty then mark the end by writing the root
            root_offset = off;
            root_offset_out[0] = off;
            memcpy(root_out, buf, write_granularity);
            printf("[LSS] Found root at offset %llu\n", (unsigned long long)off);
        }
    }

    if (FLASH_INVALID_ADDRESS == root_offset)
    {
        printf("[LSS] Warning: No root found in checkpointed segment.\n");
        free(buf);
        return LSS_ERR;
    }

    // === Step 3: Initialize LSS state ===
    lss->head = root_offset + write_granularity;
    lss->tail = lss->curr_segment_metadata.tail_offset;

    free(buf);
    return LSS_OK;
}

int lss_destroy(lss_t *lss)
{
    if (!lss)
        return LSS_ERR;
    // In this simple version, nothing special to destroy
    printf("[LSS] Destroyed log structure\n");
    free(lss->realloc_list);
    flash_destroy(lss->flash);
    return LSS_OK;
}

int lss_write(mapping_table_t * mtable, const void *data, size_t len, bool clean_tail, uint64_t * flash_address_out)
{
    if (!mtable || !data || len == 0)
    {
        assert(false);
        return LSS_ERR;
    }

    lss_t *lss = &mtable->lss;
    uint64_t flash_size = flash_get_size(lss->flash);
    uint64_t segment_size = lss->flash->segment_size_bytes;
    uint64_t used_space = lss_get_used_space(lss);
    // Compute circular distance between head and tail
    uint64_t distance = (lss->head >= lss->tail)
                            ? (flash_size - (lss->head - lss->tail))
                            : (lss->tail - lss->head);
    uint64_t threshold = 2 * segment_size;

    // Enforce at least two free blocks between head and tail
    if (used_space > (1 * segment_size))
    {
        if (clean_tail)
        {   
            int tries = 0;
            do
            {             
                // printf("[LSS] Cleaning tail block (distance=%llu)\n", (unsigned long long)distance);
                lss_clean_tail_block(mtable, lss->tail);
                used_space = lss_get_used_space(lss);

                // Recalculate distance after cleaning
                distance = (lss->head >= lss->tail)
                            ? (flash_size - (lss->head - lss->tail))
                            : (lss->tail - lss->head);
                if(++tries > 10)
                {
                    lss_debug_analyze(mtable);
                    // assert(false);
                }
            } while (distance <= threshold);
        }
        else if((len + segment_size) > distance)
        {
            assert(false);
            printf("[LSS] Insufficient distance between head and tail (distance=%llu)\n",
                   (unsigned long long)distance);
            return LSS_ERR;
        }
    }

    // Segment transition: wrap if current block is full
    // TODO: handle the case where head + len causes the new head to be inside
    // a new block causing us to lehead + lenave space unused although it is good
    if ((lss->head / segment_size) != ((lss->head + len) / segment_size))
    {
        // Move to next segment
        uint32_t next_seg = (lss->head / segment_size + 1) % (flash_size / segment_size);
        uint64_t next_seg_offset = next_seg * segment_size;
        
        lss->head = next_seg_offset;
        assert(is_mt_segment_fully_dead(mtable, next_seg_offset));
        
        // TODO: flush I/O buffers before erasing the new segmenet since a reallocated page
        // didn't get flushed and still in the buffer, and thus if we erase we will lose it
        
        //TODO: check if it is time to write a checkpoint and do it
        bool require_checkpointing = true;

        // Erase the new segment before use
        flash_erase_segment(lss->flash, next_seg_offset);

        lss_segment_metadata_t * seg_meta = &lss->curr_segment_metadata;
        
        seg_meta->contains_checkpoint = require_checkpointing; // TODO: modify later, but for now assume all segments contain a checkpoint
        
        seg_meta->block_size = mtable->block_size;
        seg_meta->npage_mappings = mtable->npage_mappings;
        
        seg_meta->tail_offset = lss->tail;
        seg_meta->seq_num++;

        lss_write_segment_metadata(lss, next_seg_offset);

        if(require_checkpointing)
        {
            lss_realloc_evict(mtable); // apply buffered realloc operations first
            // Now, this shouldn't result in an infinite recursive loop since writing the segmenet metadata results in us resovling the edge case
            // of being at the start and then trying to checkpoint and finding ourselves at the start of a new segment and so on
            // Also, only need to flush the mtable page cache since it is the only one containing ditry pages
            mtpc_flush_all(mtable->page_cache, false);

            // Now that everything has been written, mark the end of the checkpoint by writing the root page
            lss_write(mtable, mtable->root, mtable->block_size, false, &mtable->root_offset);
        }
    }

    uint64_t new_addr;
    if(lss_realloc_lookup(lss, lss->head, &new_addr))
    {
        int ret = lss_realloc_evict(mtable);
        assert(0 == ret);
    }

    // Write data at current head
    if (flash_write(lss->flash, lss->head, data, len) != 0)
    {
        assert(false);
        perror("[LSS] Flash write failed");
        return LSS_ERR;
    }

    flash_address_out[0] = lss->head;
    lss->head = lss->head + len;
    return LSS_OK;
}

int lss_read(mapping_table_t *mtable, uint64_t storage_address, void *buf, size_t len)
{
    if (!mtable || !buf)
    {
        assert(false);
        return LSS_ERR;
    }

    lss_t *lss = &mtable->lss;
    assert(lss->flash->write_granularity_bytes == len);
    uint64_t actual_address = storage_address;

    // Check if page was reallocated
    uint64_t new_addr;
    if (lss_realloc_lookup(lss, storage_address, &new_addr))
    {
        actual_address = new_addr;
    }

    // Now read from the actual location
    if (flash_read(lss->flash, actual_address, buf, len) != 0)
    {
        assert(false);
        return LSS_ERR;
    }

    return LSS_OK;
}

static uint64_t lss_get_used_space(const lss_t *lss)
{
    if (!lss || !lss->flash)
        return 0;

    // The log grows linearly from tail → head.
    // Used space = total bytes between tail and head.
    if (lss->head >= lss->tail)
        return lss->head - lss->tail;

    // Handle circular log (if head wrapped around)
    return (flash_get_size(lss->flash) - lss->tail) + lss->head;
}

static int lss_clean_tail_block(mapping_table_t * mtable, uint64_t tail_offset)
{
    if (NULL == mtable)
    {
        return LSS_ERR;
    }

    lss_t *lss = &mtable->lss;
    if (NULL == lss || NULL == lss->flash)
    {
        return LSS_ERR;
    }

    /* Use erasable block size (we are cleaning whole blocks) */
    size_t block_size = lss->flash->write_granularity_bytes;
    if (block_size == 0)
    {
        return LSS_ERR;
    }

    uint8_t *block_buf = (uint8_t *)calloc(1, block_size);
    if (NULL == block_buf)
    {
        return LSS_ERR;
    }

    /* Read entire block from flash */
    if (flash_read(lss->flash, tail_offset, block_buf, block_size) != 0)
    {
        assert(false);
        free(block_buf);
        return LSS_ERR;
    }

    flash_block_t *hdr = (flash_block_t *)block_buf;
    /* defensive: ensure header is reasonable (optional) */
    /* TODO: you can add additional validation here (magic, checksum, version) */

    /* Determine whether the mapping-table considers this block alive */
    bool alive = is_mt_block_alive(mtable, tail_offset, hdr);

    if (alive)
    {
        uint64_t new_address = 0;
        /* relocate the entire block (header + payload) to the head of the log */
        int ret = lss_write(mtable, block_buf, block_size, false, &new_address);
        if (ret != LSS_OK)
        {
            assert(false);
            free(block_buf);
            return LSS_ERR;
        }

        ret = lss_realloc_insert(mtable, hdr, tail_offset, new_address);
        if(0 != ret)
        {
            assert(false);
        }
    }

    free(block_buf);

    /* Advance tail pointer with wrap-around */
    uint64_t flash_size = flash_get_size(lss->flash);
    uint64_t new_tail = tail_offset + block_size;
    if (new_tail >= flash_size)
        new_tail = 0;

    lss->tail = new_tail;
    return LSS_OK;
}

static bool is_mt_block_alive(mapping_table_t * mtable, uint64_t block_address, flash_block_t * block_header)
{
    lss_t * lss = &mtable->lss;

    /*
        We ignore the root because a checkpoint and its associated log should only contain one root.
        During recovery, we search for that root, we assume that it reflects everything before it and we have to scan after it
        to reflect those changes

        So, if we just reallocate it, or flush it again, we might flush the root with dirty pages still in-memory, and thus
        the root will not relfect everything before it and thus some updates are lost
    */
    if((UINT32_MAX == ((uint32_t *)block_header)[0]) 
        || (SEG_META_BLOCK == block_header->type) // Because segment metadatas never need to be reallocated
        || ((MT_BLOCK == block_header->type) && (0 == block_header->level)) // This is the root, so we ignore it
        )
    {
        // all ones, then we are reading an erased block, so must be dead
        return false;
    }

    uint64_t block_start_range = block_header->logical_address;
    uint64_t block_range_size = mtable->npage_mappings_padded;
    uint32_t fanout = mtable->fanout;
    for(int i = 0; i < block_header->level; i++)
    {
        block_range_size /= fanout;
    }

    uint64_t parent_range_size = block_range_size * fanout;
    uint64_t parent_start_range = (block_start_range / parent_range_size) * parent_range_size;
    uint64_t parent_end_range = parent_start_range + parent_range_size;

    flash_block_t * curr_block = NULL;
    uint64_t curr_start_range;
    uint64_t curr_range_size;
    mtpc_entry_t * cache_entry = NULL;
    bool done = false;
    while(!done)
    {
        if(NULL == curr_block)
        {
            cache_entry = mtpc_get_page_entry(mtable->page_cache, parent_start_range, parent_end_range);
            if(NULL == cache_entry)
            {
                curr_block = mtable->root;
                curr_start_range = 0;
                curr_range_size = mtable->npage_mappings_padded; // must use padded so we don't worry about fractions or anything
            }
            else
            {
                curr_block = cache_entry->value;
                curr_start_range = cache_entry->start_range;
                curr_range_size = cache_entry->end_range - cache_entry->start_range;
            }
        }
        assert(NULL != curr_block);

        uint64_t sub_range_size = curr_range_size / fanout;
        uint32_t target_index = (block_start_range % curr_range_size) / sub_range_size;
        mapping_table_entry_t * entries = (mapping_table_entry_t *) curr_block->data;
        if(block_range_size == sub_range_size)
        {
            assert((curr_block->level + 1) == block_header->level);
            uint64_t mp_address = entries[target_index].physical_address;
            mtpc_unpin(cache_entry);
            uint64_t new_addr;
            if (lss_realloc_lookup(lss, mp_address, &new_addr))
            {
                mp_address = new_addr;
            }
            return (block_address == mp_address);
        }
        else
        {
            assert(FLASH_INVALID_ADDRESS != entries[target_index].physical_address);

            uint64_t new_entry_start_range = curr_start_range + (target_index * sub_range_size);
            curr_range_size /= fanout;
            curr_start_range = new_entry_start_range;
            mtpc_entry_t * new_cache_entry = mtpc_acquire_from_storage(lss->page_cache,
                                                                    new_entry_start_range, 
                                                                    new_entry_start_range + curr_range_size,
                                                                    entries[target_index].physical_address,
                                                                    NULL,
                                                                    false); // shouldn't flush anyway
            assert((curr_block->level + 1) < block_header->level);
            assert((curr_block->level + 1) == new_cache_entry->value->level);

            if(NULL == new_cache_entry)
            {
                assert(false);
                return false;
            }
            mtpc_unpin(cache_entry);
            curr_block = new_cache_entry->value;
            cache_entry = new_cache_entry;
        }
    }

    return false;
}

static bool apply_reallocate(mapping_table_t * mtable, flash_block_t * block_header, uint64_t old_block_address, uint64_t new_block_address)
{
    uint64_t block_start_range = block_header->logical_address;
    uint64_t block_range_size = mtable->npage_mappings_padded;
    uint32_t fanout = mtable->fanout;
    for(int i = 0; i < block_header->level; i++)
    {
        block_range_size /= fanout;
    }

    uint64_t parent_range_size = block_range_size * fanout;
    uint64_t parent_start_range = (block_start_range / parent_range_size) * parent_range_size;
    uint64_t parent_end_range = parent_start_range + parent_range_size;

    flash_block_t * curr_block = NULL;
    uint64_t curr_start_range;
    uint64_t curr_range_size;
    mtpc_entry_t * cache_entry = NULL;
    bool done = false;
    while(!done)
    {
        if(NULL == curr_block)
        {
            cache_entry = mtpc_get_page_entry(mtable->page_cache, parent_start_range, parent_end_range);
            if(NULL == cache_entry)
            {
                curr_block = mtable->root;
                curr_start_range = 0;
                curr_range_size = mtable->npage_mappings_padded; // must use padded so we don't worry about fractions or anything
            }
            else
            {
                curr_block = cache_entry->value;
                curr_start_range = cache_entry->start_range;
                curr_range_size = cache_entry->end_range - cache_entry->start_range;
            }
        }
        assert(NULL != curr_block);
        uint64_t sub_range_size = curr_range_size / fanout;
        uint32_t target_index = (block_start_range % curr_range_size) / sub_range_size;
        mapping_table_entry_t * entries = (mapping_table_entry_t *) curr_block->data;
        if(block_range_size == sub_range_size)
        {
            if(entries[target_index].physical_address == old_block_address)
            {
                // this can happen if during the time the reallocation exists in the buffer, the page gets written, which invalidates
                // the reallocation operation, and thus no longer applies
                entries[target_index].physical_address = new_block_address;
                mtpc_mark_dirty(mtable->page_cache, cache_entry);
                            
            }
            mtpc_unpin(cache_entry);
            return 0;
        }
        else
        {
            assert(FLASH_INVALID_ADDRESS != entries[target_index].physical_address);
            
            uint64_t new_entry_start_range = curr_start_range + (target_index * sub_range_size);
            curr_range_size /= fanout;
            curr_start_range = new_entry_start_range;
            mtpc_entry_t * new_cache_entry = mtpc_acquire_from_storage(mtable->page_cache,
                                                                    new_entry_start_range, 
                                                                    new_entry_start_range + curr_range_size,
                                                                    entries[target_index].physical_address,
                                                                    cache_entry,
                                                                    false); // shouldn't flush anyway
            assert((curr_block->level + 1) < block_header->level);
            assert((curr_block->level + 1) == new_cache_entry->value->level);
            assert(NULL != new_cache_entry);
            mtpc_unpin(cache_entry);
            curr_block = new_cache_entry->value;
            cache_entry = new_cache_entry;
        }
    }

    return false;
}

int lss_realloc_evict(mapping_table_t *mtable)
{
    assert(mtable);
    lss_t * lss = &mtable->lss;
    if (lss->realloc_count == 0)
    {
        return 0; // nothing to evict
    }

    size_t evict_index = 0;
    while (evict_index < lss->realloc_count)
    {
        lss_realloc_entry_t victim = lss->realloc_list[evict_index];
        int ret = apply_reallocate(mtable, &victim.block_header, victim.old_offset, victim.new_offset);
        if (0 != ret)
        {
            assert(false);
            return -1;
        }
        evict_index++;
    }
    lss->realloc_count = 0;
    return 0;
}

static int lss_realloc_insert(mapping_table_t * mtable, flash_block_t * block_header, uint64_t old_offset, uint64_t new_offset)
{
    // Prepare for sorted insertion with collapse
    lss_t * lss = &mtable->lss;
    size_t insert_pos = 0;
    for (size_t i = 0; i < lss->realloc_count; ++i)
    {
        lss_realloc_entry_t *entry = &lss->realloc_list[i];

        // Replace existing entry for the same old_offset
        if ((entry->old_offset == old_offset) || (entry->new_offset == old_offset))
        {
            assert(0 == memcmp(block_header, &entry->block_header, sizeof(flash_block_t)));
            entry->new_offset = new_offset;
            return 0;
        }

        // Track insertion point to keep list sorted
        if (entry->old_offset < old_offset)
        {
            insert_pos = i + 1;
        }
    }

    if(lss->realloc_count >= lss->realloc_capacity)
    {
        int ret = lss_realloc_evict(mtable);
        if(0 != ret)
        {
            assert(false);
            return -1;
        }
        // since we modified the array, so we have to recalculate the insert_pos
        // and yes, i know that we evict index 0 so we are basically shifting everything to the left
        // so goodjob, pat yourself on the back, and GFU, so leave it like this until we decide on the best evict policy
        insert_pos = 0;
        while (insert_pos < lss->realloc_count && lss->realloc_list[insert_pos].old_offset < old_offset)
        {
            insert_pos++;
        }
    }

    // Shift entries to make room
    memmove(&lss->realloc_list[insert_pos + 1],
            &lss->realloc_list[insert_pos],
            (lss->realloc_count - insert_pos) * sizeof(lss_realloc_entry_t));

    lss->realloc_list[insert_pos].old_offset = old_offset;
    lss->realloc_list[insert_pos].new_offset = new_offset;
    memcpy(&lss->realloc_list[insert_pos].block_header, block_header, sizeof(flash_block_t));

    lss->realloc_count++;
    return 0;
}

static bool lss_realloc_lookup(const lss_t *lss, uint64_t old_offset, uint64_t *new_offset_out)
{
    if (!lss || !lss->realloc_list)
        return false;

    uint64_t current = old_offset;
    bool found = false;
    int left = 0, right = (int)lss->realloc_count - 1;

    while (left <= right)
    {
        int mid = left + (right - left) / 2;
        uint64_t mid_val = lss->realloc_list[mid].old_offset;

        if (mid_val == current)
        {
            current = lss->realloc_list[mid].new_offset;
            found = true;
            break;
        }
        else if (mid_val < current)
            left = mid + 1;
        else
            right = mid - 1;
    }


    if (found && new_offset_out)
        *new_offset_out = current;

    return found;
}

// IMPORTANT: this updates the lss.head after writing
int lss_write_segment_metadata(lss_t * lss, uint64_t segment_offset)
{
    if(NULL == lss || NULL == lss->flash || NULL == lss)
    {
        assert(false);
        return -1;
    }
    size_t write_granularity = lss->flash->write_granularity_bytes;
    if(0 == write_granularity)
    {
        assert(false);
        return -1;
    }

    uint64_t flash_size = flash_get_size(lss->flash);
    if(segment_offset >= flash_size)
    {
        assert(false);
        return -1;
    }

    if((segment_offset % write_granularity) != 0)
    {
        assert(false);
        return -1;
    }

    // ahhhhhhhhhh make it preallocated somewhere 
    uint8_t * buf = calloc(1, write_granularity);
    if(NULL == buf)
    {
        assert(false);
        return -1;
    }

    size_t meta_size = sizeof(lss_segment_metadata_t);
    if((meta_size + sizeof(uint32_t)) > write_granularity)
    {
        assert(false && "Segment metadata too large for single write granularity slot");
        free(buf);
        return -1;
    }

    memcpy(buf, &lss->curr_segment_metadata, meta_size);

    assert(!lss_realloc_lookup(lss, segment_offset, NULL));

    int ret = flash_write(lss->flash, segment_offset, buf, write_granularity);
    if(0 != ret)
    {
        assert(false);
        free(buf);
        return -1;
    }

    lss->head = segment_offset + write_granularity;

#ifdef FLASH_DEBUG
    printf("[LSS] Wrote segment metadata: tail_offset=%lu, seq_num=%lu, block_size=%u, npage_mappings=%u\n",
        lss->curr_segment_metadata.tail_offset,
        lss->curr_segment_metadata.seq_num,
        lss->curr_segment_metadata.block_size,
        lss->curr_segment_metadata.npage_mappings);
#endif

    free(buf);
    return 0;
}

#ifdef FLASH_DEBUG

int lss_debug_analyze(mapping_table_t *mtable)
{
    if (!mtable || !mtable->lss.flash)
        return LSS_ERR;

    lss_t *lss = &mtable->lss;
    uint64_t flash_size = flash_get_size(lss->flash);
    size_t block_size = lss->flash->write_granularity_bytes;
    uint64_t n_blocks = flash_size / block_size;

    uint64_t alive_blocks = 0;
    uint64_t dead_blocks = 0;
    uint64_t invalid_blocks = 0;
    uint64_t used_space = lss_get_used_space(lss);
    uint64_t total_space = flash_size;

    uint64_t level_histogram[8] = {0}; // assuming max level < 8

    uint8_t *buf = calloc(1, block_size);
    if (!buf)
        return LSS_ERR;

    printf("\n=============================\n");
    printf("[LSS Debug Analyzer]\n");
    printf("=============================\n");
    printf("Flash size        : %llu bytes\n", (unsigned long long)flash_size);
    printf("Block size        : %zu bytes\n", block_size);
    printf("Total blocks      : %llu\n", (unsigned long long)n_blocks);
    printf("Head              : %llu\n", (unsigned long long)lss->head);
    printf("Tail              : %llu\n", (unsigned long long)lss->tail);
    printf("Used space        : %llu bytes (%.2f%%)\n",
           (unsigned long long)used_space,
           100.0 * (double)used_space / (double)total_space);

    printf("\nScanning flash contents...\n");

    for (uint64_t i = 0; i < n_blocks; i++)
    {
        uint64_t block_offset = i * block_size;
        if (flash_read(lss->flash, block_offset, buf, block_size) != 0)
        {
            invalid_blocks++;
            continue;
        }

        flash_block_t *hdr = (flash_block_t *)buf;

        // Sanity check: is it a valid block header?
        if (hdr->level > 10 || hdr->logical_address == 0)
        {
            invalid_blocks++;
            continue;
        }

        bool alive = is_mt_block_alive(mtable, block_offset, hdr);
        if (alive)
            alive_blocks++;
        else
            dead_blocks++;

        if (hdr->level < 8)
            level_histogram[hdr->level]++;
    }

    printf("\n===== LSS Summary =====\n");
    printf("Alive blocks      : %llu\n", (unsigned long long)alive_blocks);
    printf("Dead blocks       : %llu\n", (unsigned long long)dead_blocks);
    printf("Invalid blocks    : %llu\n", (unsigned long long)invalid_blocks);
    printf("Alive ratio       : %.2f%%\n",
           100.0 * (double)alive_blocks / (double)(alive_blocks + dead_blocks + invalid_blocks));

    printf("\n===== Block Level Distribution =====\n");
    for (int i = 0; i < 8; i++)
    {
        if (level_histogram[i] > 0)
            printf("Level %d: %llu blocks\n", i, (unsigned long long)level_histogram[i]);
    }

    printf("=============================\n\n");

    free(buf);
    return LSS_OK;
}

static bool is_mt_segment_fully_dead(mapping_table_t *mtable, uint64_t block_address)
{
    if (!mtable || !mtable->lss.flash)
        return false;

    lss_t *lss = &mtable->lss;
    size_t flash_block_size = lss->flash->segment_size_bytes;
    if (flash_block_size == 0)
        return false;

    uint8_t *block_buf = malloc(flash_block_size);
    if (!block_buf)
        return false;

    if (flash_read(lss->flash, block_address, block_buf, flash_block_size) != 0)
    {
        free(block_buf);
        return false;
    }

    flash_block_t *block_hdr = (flash_block_t *)block_buf;
    uint32_t page_size = lss->flash->write_granularity_bytes;
    uint32_t n_pages_in_block = flash_block_size / page_size;
    bool any_alive = false;

    // Iterate through pages (skip header size if header is stored at start)
    for (uint32_t i = 0; i < n_pages_in_block; i++)
    {
        uint64_t page_offset = block_address + i * page_size;
        if (page_offset + page_size > (block_address + flash_block_size))
        {
            assert(false);
            break; // Safety check
        }

        uint8_t *page_buf = block_buf + (page_offset - block_address);
        flash_block_t *page_hdr = (flash_block_t *)page_buf;

        // Skip erased pages (filled with 0xFF)
        if (*(uint32_t *)page_hdr == UINT32_MAX)
            continue;

        // Determine if this page is alive via mapping-table reference
        bool alive = is_mt_block_alive(mtable, page_offset, block_hdr);
        if (alive)
        {
            any_alive = true;
            break;
        }
    }

    free(block_buf);
    return !any_alive;
}

static void lss_print_realloc_list(const lss_t *lss)
{
    if (!lss || !lss->realloc_list)
    {
        printf("[LSS] No reallocation list to print.\n");
        return;
    }

    if (lss->realloc_count == 0)
    {
        printf("[LSS] Reallocation list is empty.\n");
        return;
    }

    printf("\n[LSS] Reallocation List (%zu entries):\n", lss->realloc_count);
    printf("-----------------------------------------\n");
    printf("%-5s | %-18s | %-18s\n", "Idx", "Old Offset", "New Offset");
    printf("-----------------------------------------\n");

    for (size_t i = 0; i < lss->realloc_count; i++)
    {
        const lss_realloc_entry_t *entry = &lss->realloc_list[i];
        printf("%-5zu | %llu | %llu\n",
               i,
               (unsigned long long)entry->old_offset,
               (unsigned long long)entry->new_offset);
    }

    printf("-----------------------------------------\n\n");
}


#endif