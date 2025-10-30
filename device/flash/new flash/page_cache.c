#include <flash.h>

#ifdef FLASH_DEBUG
static void mtpc_print(const mtpc_t *cache);
#endif

static int mtpc_take_entry_ownership(mtpc_t *cache_a, mtpc_entry_t *entry_a,
                                    mtpc_t *cache_b, mtpc_entry_t *entry_b);

mtpc_t *mtpc_create(uint32_t capacity, mapping_table_t * mtable, mtpc_t * parent)
{
    mtpc_t *cache = calloc(1, sizeof(mtpc_t));
    if (!cache)
    {
        return NULL;
    }

    cache->data = malloc(capacity * sizeof(mtpc_entry_t *));
    if (!cache->data)
    {
        assert(false);
        free(cache);
        return NULL;
    }

    cache->memory_arena = malloc(capacity * (sizeof(mtpc_entry_t) + mtable->block_size));
    if (!cache->memory_arena)
    {
        assert(false);
        free(cache->data);
        free(cache);
        return NULL;
    }
    cache->entry_pool = (mtpc_entry_t *)cache->memory_arena;
    cache->free_list = NULL;
    const void * blocks_pool = cache->memory_arena + capacity * sizeof(mtpc_entry_t);
    for (uint32_t i = 0; i < capacity; i++)
    {
        mtpc_entry_t *entry = &cache->entry_pool[i];
        entry->value = (flash_block_t *)(blocks_pool + (i * mtable->block_size));
        entry->value->level = 0;
        entry->value->logical_address = 0;
        entry->value->type = MT_BLOCK;

        // push to free list using lru_next temporarily
        entry->lru_next = cache->free_list;
        cache->free_list = entry;
    }

    cache->capacity = capacity;
    cache->size = 0;
    cache->lru_head = NULL;
    cache->lru_tail = NULL;
    cache->mtable = mtable;

    if(NULL != parent)
    {
        cache->parent_cache = parent;
        assert(NULL != parent);
        parent->child_cache = cache;
    }
    
    return cache;
}

// Doesn't flush ditry pages before destroying
void mtpc_destroy(mtpc_t *cache)
{
    if (!cache)
    {
        return;
    }
    free(cache->memory_arena);
    free(cache->data);
    free(cache);
}

// int mtpc_add_child(m)

static void lru_remove(mtpc_t *cache, mtpc_entry_t *entry)
{
    if(!cache || !entry)
    {
        return;
    }

    if(entry->lru_prev)
    {
        entry->lru_prev->lru_next = entry->lru_next;
    }
    else
    {
        cache->lru_head = entry->lru_next;
    }

    if(entry->lru_next)
    {
        entry->lru_next->lru_prev = entry->lru_prev;
    }
    else
    {
        cache->lru_tail = entry->lru_prev;
    }

    entry->lru_prev = NULL;
    entry->lru_next = NULL;
}

static void lru_push_head(mtpc_t *cache, mtpc_entry_t *entry)
{
    if(!cache || !entry)
    {
        return;
    }

    /* insert as most recently used */
    entry->lru_prev = NULL;
    entry->lru_next = cache->lru_head;

    if(cache->lru_head)
    {
        cache->lru_head->lru_prev = entry;
    }

    cache->lru_head = entry;

    if(!cache->lru_tail)
    {
        cache->lru_tail = entry;
    }
}

static void lru_move_to_head(mtpc_t *cache, mtpc_entry_t *entry)
{
    if(!cache || !entry)
    {
        return;
    }

    /* already head */
    if(cache->lru_head == entry)
    {
        return;
    }

    lru_remove(cache, entry);
    lru_push_head(cache, entry);
}

static mtpc_entry_t *cache_allocate_entry(mtpc_t *cache)
{
    if (!cache->free_list)
    {
        return NULL;
    }

    mtpc_entry_t *entry = cache->free_list;
    cache->free_list = entry->lru_next;

    entry->lru_prev = NULL;
    entry->lru_next = NULL;
    entry->pin_count = 0;
    entry->dirty = false;

    return entry;
}

static void cache_free_entry(mtpc_t *cache, mtpc_entry_t *entry)
{
    if (!entry)
    {
        return;
    }

    entry->start_range = 0;
    entry->end_range = 0;
    entry->parent = NULL;
    entry->pin_count = 0;
    entry->dirty = false;

    entry->lru_next = cache->free_list;
    cache->free_list = entry;
}

static int entry_cmp_ptr(const void *pa, const void *pb)
{
    const mtpc_entry_t *a = *(const mtpc_entry_t *const *)pa;
    const mtpc_entry_t *b = *(const mtpc_entry_t *const *)pb;

    if (a->start_range < b->start_range)
    {
        return -1;
    }

    if (a->start_range > b->start_range)
    {
        return 1;
    }

    uint64_t span_a = a->end_range - a->start_range;
    uint64_t span_b = b->end_range - b->start_range;

    if (span_a < span_b)
    {
        return -1;
    }

    if (span_a > span_b)
    {
        return 1;
    }

    return 0;
}

int flush_entry(mtpc_t *cache, mtpc_entry_t * entry, bool clean_tail)
{
    uint64_t storage_address;
    uint64_t curr_range;

    int ret = lss_write(cache->mtable, entry->value, cache->mtable->block_size, clean_tail, &storage_address);
    if(ret != 0)
    {
        assert(false);
        return -1;
    }
    if(!entry->dirty)
    {
        // We experienced a checkpoint and this page has already been flushed so do nothing and return
        return 0;
    }
    
    entry->dirty = false;
    
    flash_block_t * parent_block = NULL;
    if(entry->parent)
    {
        parent_block = entry->parent->value;
        curr_range = entry->parent->end_range - entry->parent->start_range;
        entry->parent->dirty = true;
    }
    else
    {
        parent_block = cache->mtable->root;
        curr_range = cache->mtable->npage_mappings_padded;
    }

    assert(parent_block != NULL);

    uint64_t sub_range = curr_range / cache->mtable->fanout;
    uint32_t target_index = (entry->start_range % curr_range) / sub_range;

    mapping_table_entry_t * entries = (mapping_table_entry_t *) parent_block->data;
    entries[target_index].physical_address = storage_address;

    return 0;
}

// This inserts and pins the entry
// the user must unpin the page
// the block associated with the returned entry might have been used
// before and containing some garbage, so either fully overwrite
// before use or zero it out
mtpc_entry_t * mtpc_insert(mtpc_t *cache,
                        uint64_t start_range,
                        uint64_t end_range,
                        uint8_t level,
                        mtpc_entry_t *parent,
                        bool clean_tail)
{
    if(!cache)
    {
        return NULL;
    }

    mtpc_entry_t *entry = NULL;

    // Sooo, there's a case, a very lucky one at that, that as we allocate an entry, we evict a dirty one, that results in us
    // apply a reallocate operation that loads the page we want to acquire in memory, so reading it again
    // will overwrite the effects of the reallocate operation and will be useless
    // so we have to do a quick check that we still don't have the target page in-memory

    // try to make sure that we are not already inserting an entry that exists
    // no need to pin it, since get entry already does so
    entry = mtpc_get_page_entry(cache, start_range, end_range);
    if((NULL != entry) && (entry->start_range == start_range) && (entry->end_range == end_range))
    {
        lru_move_to_head(cache, entry);
        return entry;
    }
    else if(NULL != entry)
    {
        // not our entry, make sure to unpin it
        mtpc_unpin(entry);
    }

    entry = cache_allocate_entry(cache);
    if(!entry)
    {
        mtpc_entry_t *cand = NULL;
        do
        {
            if(NULL != cand)
            {
                lru_push_head(cache, cand);
            }

            cand = cache->lru_tail;
            while(cand)
            {
                if(cand->pin_count == 0)
                {
                    break;
                }
                cand = cand->lru_prev;
            }

            if(!cand)
            {
                assert(false); /* nothing evictable */
                return NULL;
            }

            lru_remove(cache, cand);

            if(cand->dirty)
            {
                int ret = flush_entry(cache, cand, clean_tail);
                if(0 != ret)
                {
                    assert(false);
                    return NULL;
                }
            }
            assert(!cand->dirty);
        } while (0 != cand->pin_count);

        mtpc_entry_t * spawned = mtpc_get_page_entry(cache, start_range, end_range);
        if((NULL != spawned) && (spawned->start_range == start_range) && (spawned->end_range == end_range))
        {
            lru_push_head(cache, cand);
            lru_move_to_head(cache, spawned);
            return spawned;
        }
        else if(NULL != cand)
        {
            // not our entry, make sure to unpin it
            mtpc_unpin(spawned);
        }

        assert(NULL != cand);
        assert(0 == cand->pin_count);

        {
            /* remove from sorted array */
            uint32_t idx = UINT32_MAX;
            for(uint32_t i = 0; i < cache->size; i++)
            {
                if(cache->data[i] == cand)
                {
                    idx = i;
                    break;
                }
            }

            assert(idx != UINT32_MAX);

            memmove(&cache->data[idx],
                    &cache->data[idx + 1],
                    (cache->size - idx - 1) * sizeof(mtpc_entry_t *));
            cache->size--;
        }

        if(cand->parent)
        {
            assert((cand->parent->value->level + 1) == cand->value->level);
            assert(0 < cand->parent->pin_count);
            cand->parent->pin_count--;
        }
        entry = cand;
    }
    assert(NULL != entry);

    entry->start_range = start_range;
    entry->end_range = end_range;
    entry->parent = parent;
    entry->value->level = level;
    entry->value->type = MT_BLOCK;
    entry->value->logical_address = start_range;
    entry->pin_count = 1;
    entry->dirty = false;

    if(parent)
    {
        parent->pin_count++;
    }

    /* find sorted position */
    int left = 0;
    int right = cache->size;

    while(left < right)
    {
        int mid = left + (right - left) / 2;
        int c = entry_cmp_ptr(&entry, &cache->data[mid]);

        if(c > 0)
        {
            left = mid + 1;
        }
        else
        {
            assert(0 != c);
            right = mid;
        }
    }

    uint32_t idx = left;

    if(idx < cache->size)
    {
        memmove(&cache->data[idx + 1],
                &cache->data[idx],
                (cache->size - idx) * sizeof(mtpc_entry_t *));
    }

    cache->data[idx] = entry;
    cache->size++;

    lru_push_head(cache, entry);
    assert((NULL == parent) || (0 < parent->pin_count));

    return entry;
}

// if an entry is found we pin it before we return it
mtpc_entry_t *mtpc_search(mtpc_t *cache, uint64_t logical_address)
{
    if(!cache || cache->size == 0)
    {
        return NULL;
    }

    mtpc_entry_t *best = NULL;
    uint64_t best_span = UINT64_MAX;

    for(uint32_t i = 0; i < cache->size; i++)
    {
        mtpc_entry_t *entry = cache->data[i];

        if(entry->end_range <= logical_address)
        {
            continue;
        }

        if(logical_address >= entry->start_range && logical_address < entry->end_range)
        {
            uint64_t span = entry->end_range - entry->start_range;
            if(span < best_span)
            {
                best = entry;
                best_span = span;
            }
        }
    }

    if(best)
    {
        /* mark as most recently used */
        lru_move_to_head(cache, best);
        best->pin_count++;  // pin the entry
    }

    return best;
}

// if an entry is found we pin it before we return it
mtpc_entry_t * mtpc_get_page_entry(mtpc_t *cache, uint64_t start_range, uint64_t end_range)
{
    if(!cache || cache->size == 0)
    {
        return NULL;
    }

    mtpc_entry_t *best = NULL;
    uint64_t best_span = UINT64_MAX;

    for(uint32_t i = 0; i < cache->size; i++)
    {
        mtpc_entry_t * entry = cache->data[i];

        if(entry->end_range <= start_range)
        {
            continue;
        }

        if((start_range >= entry->start_range && start_range < entry->end_range)
            && (end_range > entry->start_range && end_range <= entry->end_range))
        {
            uint64_t span = entry->end_range - entry->start_range;
            if(span < best_span)
            {
                best = entry;
                best_span = span;
            }
        }
    }

    if(best)
    {
        best->pin_count++;  // pin the entry
    }

    return best;
}

// acquire from storage using preallocated entry
mtpc_entry_t *mtpc_acquire_from_storage(mtpc_t *cache,
                                    uint64_t start_range,
                                    uint64_t end_range,
                                    uint64_t storage_address,
                                    mtpc_entry_t *parent,
                                    bool clean_tail)
{
    if(!cache || (FLASH_INVALID_ADDRESS == storage_address))
    {
        assert(false);
        return NULL;
    }

    // setting the level to all ones (-1), so that we can then determine whether the entry we got was created by us,
    // or truly a lucky entry
    mtpc_entry_t *entry = mtpc_insert(cache, start_range, end_range, 0, parent, clean_tail);
    if(!entry)
    {
        assert(false);
        return NULL;
    }

    bool read_from_flash = true;
    if(NULL == cache->parent_cache)
    {
        // Page spawned or already exists in cache
        if(0 != entry->value->level)
        {
            // We are the parant, so there can't be a more recent version
            return entry;
        }

        // Now, the page didn't exist, so we check if it exists in the child
        mtpc_entry_t * child_entry = mtpc_get_page_entry(cache->child_cache, start_range, end_range);
        if((NULL != child_entry) && (child_entry->start_range == start_range) && (child_entry->end_range == end_range))
        {
            // turns out the page already exists in the child, so we give the ownership the ownership of the
            // of the freed entry to the child, and take the onwership of the child entry
            // no need to pin or do anything since both should have already been pinned by previous operation
            mtpc_take_entry_ownership(cache, entry, cache->child_cache, child_entry);
            assert(!child_entry->parent);
            // should have already been pinned in mtpc_insert
            child_entry->parent = parent;
            read_from_flash = false;
            entry = child_entry;
        }
        else
        {
            // not ours so we unpin it and continue normally
            mtpc_unpin(child_entry);
        }
    }
    else
    {
        // Page spawned or already exists in cache
        if(0 != entry->value->level)
        {
            return entry;
        }
        // i am a child (yes you are)
        mtpc_entry_t * parent_entry = mtpc_get_page_entry(cache->parent_cache, start_range, end_range);
        if((NULL != parent_entry) && (parent_entry->start_range == start_range) && (parent_entry->end_range == end_range))
        {
            // it is the entry we need so we simply utilize as it is
            read_from_flash = false;
            entry = parent_entry;
        }
        else
        {
            // not ours so we unpin it and continue normally
            mtpc_unpin(parent_entry);
        }
    }

    if(read_from_flash)
    {
        int ret = lss_read(cache->mtable, storage_address, entry->value, cache->mtable->block_size);
        if(ret != 0)
        {
            // return entry to free list
            assert(false);
            cache_free_entry(cache, entry);
            return NULL;
        }
    }

    assert(UINT32_MAX != ((uint32_t *)entry->value)[0]);
    assert(entry->value->type == MT_BLOCK);
    assert(entry->value->logical_address == start_range);
    assert(!entry->dirty);
    return entry;
}

uint16_t mtpc_unpin(mtpc_entry_t *entry)
{
    if(!entry)
    {
        return 0;
    }

    assert(entry->pin_count > 0);
    return --entry->pin_count;
}

void mtpc_mark_dirty(mtpc_t *cache, mtpc_entry_t * entry)
{
    (void)cache;
    if(!entry)
    {
        return;
    }
    entry->dirty = true;
}

/*
    This function gives cache_a the ownership of entry_b treating it as a live entry
    and gives cache_b the ownership of entry_a but treats it as a free entry

    The reason for this is that we can't take the ownership of something without something in return
*/
static int mtpc_take_entry_ownership(mtpc_t *cache_a, mtpc_entry_t *entry_a,
                                    mtpc_t *cache_b, mtpc_entry_t *entry_b)
{
    if (!cache_a || !entry_a || !cache_b || !entry_b)
    {
        assert(false);
        return -1;
    }

    // --- Step 1: Remove both entries from their caches ---
    // Remove from LRU
    lru_remove(cache_a, entry_a);
    lru_remove(cache_b, entry_b);

    // Remove from sorted data[]
    uint32_t idx_a = UINT32_MAX, idx_b = UINT32_MAX;

    for (uint32_t i = 0; i < cache_a->size; i++)
    {
        if (cache_a->data[i] == entry_a)
        {
            idx_a = i;
            break;
        }
    }
    for (uint32_t i = 0; i < cache_b->size; i++)
    {
        if (cache_b->data[i] == entry_b)
        {
            idx_b = i;
            break;
        }
    }

    if (idx_a == UINT32_MAX || idx_b == UINT32_MAX)
    {
        assert(false && "Entries not found in their respective caches");
        return -1;
    }

// Do we really need to remove it or can we just reuse the entry?
    memmove(&cache_a->data[idx_a],
            &cache_a->data[idx_a + 1],
            (cache_a->size - idx_a - 1) * sizeof(mtpc_entry_t *));
    memmove(&cache_b->data[idx_b],
            &cache_b->data[idx_b + 1],
            (cache_b->size - idx_b - 1) * sizeof(mtpc_entry_t *));

    cache_a->size--;
    cache_b->size--;

    // --- Step 2: Insert each entry into the other cache ---
    // (preserving sorted order)
    int left_b = 0, right_b = cache_a->size;
    while (left_b < right_b)
    {
        int mid = left_b + (right_b - left_b) / 2;
        int c = entry_cmp_ptr(&entry_b, &cache_a->data[mid]);
        if (c > 0)
            left_b = mid + 1;
        else
            right_b = mid;
    }


    if ((uint32_t)left_b < cache_a->size)
    {
        memmove(&cache_a->data[left_b + 1],
                &cache_a->data[left_b],
                (cache_a->size - left_b) * sizeof(mtpc_entry_t *));
    }
    cache_a->data[left_b] = entry_b;
    cache_a->size++;

    // --- Step 3: Reattach to caches ---
    lru_push_head(cache_a, entry_b);
    cache_free_entry(cache_b, entry_a);
    
    return 0;
}

int mtpc_flush_all(mtpc_t *cache, bool clean_tail)
{
    if (!cache)
    {
        return -1;
    }

    int total_flushed = 0;
    // Determine the maximum level in this cache
    uint8_t max_level = 0;
    for (uint32_t i = 0; i < cache->size; i++)
    {
        if (cache->data[i] && cache->data[i]->value->level > max_level)
        {
            max_level = cache->data[i]->value->level;
        }
    }

    // Flush from deepest to shallowest (bottom-up)
    for (int level = max_level; level >= 0; level--)
    {
        for (uint32_t i = 0; i < cache->size; i++)
        {
            mtpc_entry_t *entry = cache->data[i];
            if ((!entry) || (!entry->dirty) || (entry->value->level != level))
            {
                continue;
            }

            int ret = flush_entry(cache, entry, clean_tail);
            if (ret != 0)
            {
                assert(false);
                fprintf(stderr, "[MTPC] Failed to flush entry [%llu,%llu) level=%u\n",
                        (unsigned long long)entry->start_range,
                        (unsigned long long)entry->end_range,
                        entry->value->level);
                return -1;
            }
            entry->dirty = false;
            total_flushed++;
        }
    }
#ifdef FLASH_DEBUG
    {
        /* free all heap entries */
        for(uint32_t i = 0; i < cache->size; i++)
        {
            mtpc_entry_t *entry = cache->data[i];
            if(entry && entry->dirty)
            {
                /* if dirty and flush_cb exists, flush */
                assert(false);
            }
        }   
    }
#endif
    return total_flushed;
}



#ifdef FLASH_DEBUG
static void mtpc_print(const mtpc_t *cache)
{
    if (!cache)
    {
        printf("[MTPC] cache is NULL\n");
        return;
    }

    printf("\n==============================\n");
    printf("[Mapping Table Page Cache Dump]\n");
    printf("==============================\n");

    printf("Capacity     : %u\n", cache->capacity);
    printf("Current size : %u\n", cache->size);
    printf("Free entries : ");

    // Count free list length
    uint32_t free_count = 0;
    for (mtpc_entry_t *f = cache->free_list; f; f = f->lru_next)
        free_count++;
    printf("%u\n", free_count);

    printf("\n--- Entries (sorted by range) ---\n");
    if (cache->size == 0)
    {
        printf("(empty)\n");
    }
    else
    {
        for (uint32_t i = 0; i < cache->size; i++)
        {
            const mtpc_entry_t *e = cache->data[i];
            if (!e || !e->value)
                continue;

            printf("[%02u] range=[%llu, %llu) level=%u pins=%u dirty=%d ",
                   i,
                   (unsigned long long)e->start_range,
                   (unsigned long long)e->end_range,
                   e->value->level,
                   e->pin_count,
                   e->dirty);

            if (e->parent)
                printf("parent_start=%llu ",
                       (unsigned long long)e->parent->start_range);

            printf("addr=%p\n", (void *)e);
        }
    }

    printf("\n--- LRU order (head → tail) ---\n");
    {
        const mtpc_entry_t *curr = cache->lru_head;
        int idx = 0;
        while (curr)
        {
            printf("(%02d) range=[%llu,%llu) pins=%u dirty=%d\n",
                   idx++,
                   (unsigned long long)curr->start_range,
                   (unsigned long long)curr->end_range,
                   curr->pin_count,
                   curr->dirty);
            curr = curr->lru_next;
        }

        if (idx == 0)
            printf("(empty)\n");
    }

    printf("\n==============================\n\n");
}
#endif