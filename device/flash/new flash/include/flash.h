#ifndef FLASH_H
#define FLASH_H

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <stdbool.h>
#include <stdint.h>
#include <assert.h>
#include <string.h>

#define MTPC_CAPACITY       8

#define FLASH_DEFAULT_BLOCK_SIZE 4096
#define FLASH_DEFAULT_WRITE_GRANULARITY 64

#define FLASH_TOTAL_SIZE    (FLASH_DEFAULT_BLOCK_SIZE * 6)

#define FLASH_INVALID_ADDRESS       UINT64_MAX

typedef struct flash_s flash_t;
typedef struct mapping_table_s mapping_table_t;
typedef struct mtpc_s mtpc_t;

#define FLASH_DEBUG

// Simple file-backed flash simulator
struct flash_s
{
    int fd;
    uint64_t size_bytes;
    uint32_t segment_size_bytes;
    uint32_t write_granularity_bytes;
};

enum flash_block_type_t
{
    MT_BLOCK = 0,
    DATA_BLOCK = 1,
    SEG_META_BLOCK = 2,
};

typedef struct flash_block_s
{
    uint64_t type : 2;
    // if we are working with a data page, then the rest of the bits are the logical address,
    // if we are working with a mapping table page, then it is split into level and start range
    uint64_t level: 6;              // root is 0
    // if this is "not" an mt page, then this is the logical address of the page
    // else, then this is the start range of the page and in the data we should know the level of the page
    uint64_t logical_address: 56;
    uint8_t data[];                  /**< buffer for this block               */
} flash_block_t;

/* entry object (heap allocated) */
typedef struct mtpc_entry_s
{
    // REPEATATION AHH, AND THE START RANGE IS ALSO EASLY ACCESSABLE THROUGH THE PAGE HEADER
    // CLEAN LATER
    uint64_t start_range;
    uint64_t end_range;
    flash_block_t * value;

    /* parent pointer: points to parent entry object (or NULL) */
    struct mtpc_entry_s *parent;

    /* pin count: >0 means pinned (in use), cannot be evicted */
    uint16_t pin_count;

    /* dirty flag: needs write-back before eviction */
    bool dirty;

    /* LRU doubly-linked list pointers (stable - stored inside entry) */
    struct mtpc_entry_s *lru_prev;
    struct mtpc_entry_s *lru_next;
} mtpc_entry_t;


typedef struct lss_realloc_entry_s lss_realloc_entry_t;

typedef struct lss_segment_metadata_s
{
    uint8_t type : 2;
    uint8_t contains_checkpoint : 1;
    
    uint32_t block_size;
    uint32_t npage_mappings;

    uint64_t tail_offset;
    // uint64_t segment_index;  /* logical index (optional) */
    uint64_t seq_num;      /* optional: epoch time or monotonic counter */
} lss_segment_metadata_t;

typedef struct lss_s
{
    flash_t *flash; // underlying flash device
    uint64_t head;
    uint64_t tail;

    lss_realloc_entry_t * realloc_list;
    size_t realloc_count;     // number of valid entries
    size_t realloc_capacity;  // allocated size

    // This is used to load and store the pages that are needed to determine
    // whether a page is still alive or not
    // This is only working as a page pool, and all the pages inside of it
    // are assumed to be clean, so this doesn't result in any writes of its own
    mtpc_t * page_cache;

    lss_segment_metadata_t curr_segment_metadata;
} lss_t;


/* cache stores array of pointers to entries plus LRU head/tail */
// mapping table page cache
struct mtpc_s
{
    /*
        Soooo, currently the cache pins the entire path in-memory, this is "I THINKKK" is better for the usage of the
        mapping table, since it modifies the page
        but for the lss, which only reads, there is no reason to do so. So, I think we should add some configuration flags
        things like: flush_dirty, pin_parent and some other options that might be needed
    */
    uint32_t capacity;
    uint32_t size;

    mtpc_entry_t **data; /* dynamic array of pointers, sorted by (start, span) */
    /* LRU list: head = MRU, tail = LRU */
    mtpc_entry_t *lru_head;
    mtpc_entry_t *lru_tail;

    mtpc_entry_t *entry_pool;
    mtpc_entry_t *free_list;

    mapping_table_t * mtable;

    void * memory_arena;

    // A cache can have child caches, but the parent is the only one that can R/W, while
    // the children can only read.
    // The idea is that a parent and hit children can share their entries and the idea is that we ensure that a single version
    // of a page can only exist in-memory at a time and thus preventing any inconsistency between the caches and the flash from
    // emerging in the first place
    struct mtpc_s * child_cache;
    struct mtpc_s * parent_cache;
};


typedef struct mapping_table_entry_s
{
    // no need to store the logical address for each entry, since it is basically
    // the start range of the page plus the index of the entry in the page
    uint64_t physical_address;
} mapping_table_entry_t;

struct mapping_table_s
{
    uint32_t block_size;
    
    // root page which is always in-memory
    flash_block_t * root;
    uint64_t root_offset;

    // total number of entries (logical to physical mappings)
    // note: this needs to be updated as flash blocks get worn out so
    // that we never allocate more than the flash can handle
    uint32_t npage_mappings;

    // this gives the total number of entries, given the height of the mapping table tree and assuming the tree is complete
    // which it doesn't necesseracly have to be, but we need this alot so we precalculate it and store it here
    // also this is fixed and never changes 
    uint32_t npage_mappings_padded;

    // number of allocated logical pages
    uint32_t nallocated_pages;

    // uint32_t mapping_table_pages_size;

    // once set after the creation of the mapping table never changes
    // since we are working with a static heircal structure and not a self balancing one
    uint32_t fanout;

    // TODO: mapping table page cache
    mtpc_t * page_cache;

    lss_t lss;
    
    // Hmm, should we have a cache for the entries as well? or should we just work with the entries directly in the page cache?
};


mtpc_t *mtpc_create(uint32_t capacity, mapping_table_t * mtable, mtpc_t * parent);
void mtpc_destroy(mtpc_t * cache);
mtpc_entry_t * mtpc_search(mtpc_t *const cache, uint64_t logical_address);
mtpc_entry_t * mtpc_get_page_entry(mtpc_t *cache, uint64_t start_range, uint64_t end_range);
mtpc_entry_t * mtpc_insert(mtpc_t *cache,
                        uint64_t start_range,
                        uint64_t end_range,
                        uint8_t level,
                        mtpc_entry_t *parent,
                        bool clean_tail);
mtpc_entry_t *mtpc_acquire_from_storage(mtpc_t *cache,
                                    uint64_t start_range,
                                    uint64_t end_range,
                                    uint64_t storage_address,
                                    mtpc_entry_t *parent,
                                    bool clean_tail);
int mtpc_flush_all(mtpc_t *cache, bool clean_tail);
uint16_t mtpc_unpin(mtpc_entry_t * entry);
void mtpc_mark_dirty(mtpc_t *cache, mtpc_entry_t * entry);

mapping_table_t * mapping_table_init(const char * name, const uint32_t npage_mappings, const uint32_t page_size, const uint32_t page_cache_capacity, bool create);
void mapping_table_destroy(mapping_table_t * mtable);
int mapping_table_update_physical_address(mapping_table_t * mtable, uint64_t logical_address, uint64_t physical_address, bool clean_tail);
int mapping_table_get_physical_address(mapping_table_t *mtable, uint64_t logical_address, bool clean_tail, uint64_t * physical_address);

int lss_create(lss_t *lss, mtpc_t * page_cache, const char * name);
int lss_init(lss_t *lss, mapping_table_t * mtable);
int lss_load_root_and_seg_meta(lss_t *lss, flash_block_t * root_out, uint64_t * root_offset_out);
int lss_realloc_evict(mapping_table_t *mtable);
int lss_destroy(lss_t *lss);

int lss_write(mapping_table_t * mtable, const void *data, size_t len, bool clean_tail, uint64_t * flash_address_out);
int lss_read(mapping_table_t * mtable, uint64_t offset, void * buf, size_t len);


#ifdef FLASH_DEBUG
int lss_debug_analyze(mapping_table_t *mtable);
static inline void mtpc_print_entry(const flash_block_t * block)
{
    if (!block)
    {
        printf("[MTPC] (null entry)\n");
        return;
    }

    printf("──────────────────────────────────────────────\n");

    printf(" Is MT Page: %d\n", block->type);
    printf(" Level %d / Start Range: %lu\n", (int)block->level, (uint64_t)block->logical_address);


    // Optionally dump the first few mapping entries if it's an MT page
    
    const mapping_table_entry_t *entries =
        (const mapping_table_entry_t *)block->data;

    // Show first few entries
    printf(" Data:\n");
    for (int i = 0; i < 7; ++i)
    {
        printf("   [%d] phys=%llu\n",
            i, (unsigned long long)entries[i].physical_address);
        // assert(131072 > entries[i].physical_address);
    }


    printf("──────────────────────────────────────────────\n");
}
#endif

#endif // FLASH_H