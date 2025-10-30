#ifndef IO_WRAPPER_H
#define IO_WRAPPER_H

#include "flash.h"
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdint.h>
#include <stddef.h>


// ==============================
// Allocation / lifecycle
// ==============================
static inline flash_t * flash_open(const char *path, uint64_t size_bytes)
{
    flash_t *flash = calloc(1, sizeof(flash_t));
    if (!flash)
    {
        assert(false);
        return NULL;
    }

    if (!flash || !path)
    {
        assert(false);
        return NULL;
    }

    flash->fd = open(path, O_RDWR | O_CREAT, 0644);
    if (flash->fd < 0)
    {
        printf("%s\n", strerror(errno));
        assert(false);
        perror("open");
        return NULL;
    }

    flash->size_bytes = size_bytes;
    flash->segment_size_bytes = FLASH_DEFAULT_BLOCK_SIZE;
    flash->write_granularity_bytes = FLASH_DEFAULT_WRITE_GRANULARITY;
    return flash;
}

static inline void flash_destroy(flash_t *flash)
{
    if (!flash)
        return;
    if (flash->fd >= 0)
        close(flash->fd);
    flash->fd = -1;
    free(flash);
}

static inline int flash_read(flash_t *flash, uint64_t addr, void *buf, size_t len)
{
    if (!flash || addr + len > flash->size_bytes)
    {
        assert(false);
        return -1;
    }

    ssize_t ret = pread(flash->fd, buf, len, addr);
    if (ret != (ssize_t)len)
    {
        printf("%s\n", strerror(errno));
        fflush(NULL);
        assert(false);
        perror("pread");
        return -1;
    }
    return 0;
}

static inline int flash_write(flash_t *flash, uint64_t addr, const void *buf, size_t len)
{
    if (!flash || addr + len >= flash->size_bytes)
    {
        assert(false);
        return -1;
    }

    ssize_t ret = pwrite(flash->fd, buf, len, addr);
    if (ret != (ssize_t)len)
    {
        assert(false);
        perror("pwrite");
        return -1;
    }

    fsync(flash->fd);
    return 0;
}

static inline int flash_erase_segment(flash_t *flash, uint64_t segment_offset)
{
    if (!flash)
    {
        return -1;
    }

    uint32_t segment_size = flash->segment_size_bytes;

    if (segment_offset >= flash->size_bytes || segment_offset % segment_size != 0)
    {
        fprintf(stderr, "[FLASH] Invalid block offset: %llu\n",
                (unsigned long long)segment_offset);
        return -1;
    }

    void *ones = malloc(flash->segment_size_bytes);
    if (!ones)
        return -1;
    memset(ones, 0xFF, flash->segment_size_bytes);

    ssize_t ret = pwrite(flash->fd, ones, segment_size, segment_offset);
    if (ret != (ssize_t)segment_size)
    {
        perror("erase");
        free(ones);
        return -1;
    }

    fsync(flash->fd);
    free(ones);
    return 0;
}


static inline int flash_erase_all(flash_t *flash)
{
    if (!flash)
        return -1;

    void *ones = malloc(flash->segment_size_bytes);
    if (!ones)
        return -1;
    memset(ones, 0xFF, flash->segment_size_bytes);

    for (uint64_t offset = 0; offset < flash->size_bytes; offset += flash->segment_size_bytes)
    {
        if (pwrite(flash->fd, ones, flash->segment_size_bytes, offset) != (ssize_t)flash->segment_size_bytes)
        {
            perror("erase");
            free(ones);
            return -1;
        }
    }

    fsync(flash->fd);
    free(ones);
    return 0;
}

static inline uint32_t flash_get_segment_size_bytes(flash_t *flash)
{
    assert(NULL != flash);
    return flash->segment_size_bytes;
}

static inline uint32_t flash_get_write_granularity(flash_t *flash)
{
    assert(NULL != flash);
    return flash->write_granularity_bytes;
}

static inline uint64_t flash_get_size(flash_t *flash)
{
    assert(NULL != flash);
    return flash->size_bytes;
}


#endif // IO_WRAPPER_H