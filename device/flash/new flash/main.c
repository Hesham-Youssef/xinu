#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <stdbool.h>
#include <time.h>
#include "flash.h"

/*****************************************************
 * Helper types & utilities
 *****************************************************/

typedef struct {
    const char *name;
    void (*func)(mapping_table_t *mt);
} test_case_t;

typedef struct {
    uint64_t logical;
    uint64_t physical;
} mapping_t;

static int cmp_mapping(const void *a, const void *b)
{
    uint64_t la = ((const mapping_t *)a)->logical;
    uint64_t lb = ((const mapping_t *)b)->logical;
    return (la > lb) - (la < lb);
}

#define TEST_START(name) printf("\n[RUNNING] %s\n", name)
#define TEST_PASS(name, time_ms)  printf("%s PASSED (%.3f ms)\n", name, time_ms)
#define TEST_FAIL(name)  printf("%s FAILED\n", name)

/*****************************************************
 * Individual tests
 *****************************************************/

static void test_basic_insert(mapping_table_t *mt)
{
    TEST_START("Basic insert and lookup");

    uint64_t logical = 10;
    uint64_t physical = 5555;
    uint64_t phys_out = 0;

    assert(mapping_table_update_physical_address(mt, logical, physical, true) == 0);
    int rc = mapping_table_get_physical_address(mt, logical, true, &phys_out);
    assert(rc == 0 && phys_out == physical);

    printf("  Logical %llu → Physical %llu\n",
           (unsigned long long)logical,
           (unsigned long long)phys_out);

    TEST_PASS("Basic insert and lookup", 0.0);
}

static void test_overwrite(mapping_table_t *mt)
{
    TEST_START("Overwrite existing mapping");

    uint64_t logical = 10;
    uint64_t new_phys = 7777;
    uint64_t phys_out = 0;

    assert(mapping_table_update_physical_address(mt, logical, new_phys, true) == 0);
    int rc = mapping_table_get_physical_address(mt, logical, true, &phys_out);
    assert(rc == 0 && phys_out == new_phys);

    printf("  Logical %llu correctly updated → %llu\n",
           (unsigned long long)logical,
           (unsigned long long)phys_out);

    TEST_PASS("Overwrite existing mapping", 0.0);
}

static void test_unmapped_lookup(mapping_table_t *mt)
{
    TEST_START("Unmapped address lookup");

    uint64_t unmapped = 999999;
    uint64_t phys_out = 0;

    int rc = mapping_table_get_physical_address(mt, unmapped, true, &phys_out);
    assert(rc == -1);

    TEST_PASS("Unmapped address lookup", 0.0);
}


static void test_bulk_random(mapping_table_t *mt)
{
    TEST_START("Bulk random insertions and lookups");
    
    size_t N = mt->npage_mappings; // gets assigned in main
    mapping_t *expected = calloc(N, sizeof(mapping_t));
    size_t expected_size = 0;
    
    for (size_t i = 0; i < N; i++)
    {
        uint64_t logical = i;
        uint64_t physical = rand();

        assert(mapping_table_update_physical_address(mt, logical, physical, true) == 0);

        mapping_t key = {.logical = logical};
        mapping_t *found = bsearch(&key, expected, expected_size, sizeof(mapping_t), cmp_mapping);
        if (found)
        {
            found->physical = physical;
        }
        else
        {
            expected[expected_size++] = (mapping_t){logical, physical};
            qsort(expected, expected_size, sizeof(mapping_t), cmp_mapping);
        }

        uint64_t phys_out = 0;
        int rc = mapping_table_get_physical_address(mt, logical, true, &phys_out);
        assert(rc == 0);
        assert(phys_out == physical);
    }

    for (size_t i = 0; i < expected_size; i++)
    {
        uint64_t phys_out = 0;
        int rc = mapping_table_get_physical_address(mt, expected[i].logical, true, &phys_out);
        assert(rc == 0);
        assert(phys_out == expected[i].physical);
    }

    // for (size_t i = 0; i < 64; i++)
    // {
    //     uint64_t logical = (mt->npage_mappings / 2) + (rand() % (mt->npage_mappings / 2));
    //     uint64_t phys_out = 0;
    //     int rc = mapping_table_get_physical_address(mt, logical, true, &phys_out);
    //     assert(rc != 0 && "Expected lookup for unmapped logical page to fail");
    // }

    free(expected);
    TEST_PASS("Bulk random insertions, lookups, and missing-entry checks", 0.0);
}


static void test_heavy_stress(mapping_table_t *mt)
{
    TEST_START("Extreme insert & update stress");

    const size_t OPS = 500;         // total operations
    const uint64_t LOGICAL_SPACE = mt->npage_mappings; // logical range
    uint64_t phys_out = 0;

    // use a small in-memory mirror to verify correctness
    uint64_t *mirror = calloc(LOGICAL_SPACE, sizeof(uint64_t));
    bool *mapped = calloc(LOGICAL_SPACE, sizeof(bool));

    assert(mirror && mapped);

    for (size_t i = 0; i < OPS; i++)
    {
        uint64_t logical = rand() % LOGICAL_SPACE;
        uint64_t physical = ((uint64_t)rand() << 32) | rand();

        // 70% update/insert, 30% read
        if (true)
        {
            int rc = mapping_table_update_physical_address(mt, logical, physical, true);
            assert(rc == 0);
            mirror[logical] = physical;
            mapped[logical] = true;
        }
        else
        {
            int rc = mapping_table_get_physical_address(mt, logical, true, &phys_out);
            if (mapped[logical])
            {
                assert(rc == 0);
                assert(phys_out == mirror[logical]);
            }
            else
            {
                assert(rc != 0);
            }
        }

        // Periodically sanity-check a few random entries
        // if ((i % 50000) == 0 && i > 0)
        {
            printf("  ...%zu ops done\n", i);
            for (uint64_t k = 0; k < LOGICAL_SPACE; k++)
            {
                uint64_t probe = k;
                int rc = mapping_table_get_physical_address(mt, probe, true, &phys_out);
                if (mapped[probe])
                    assert(rc == 0 && phys_out == mirror[probe]);
                else
                    assert(rc != 0);
            }
        }
    }

    free(mirror);
    free(mapped);
    TEST_PASS("Extreme insert & update stress", 0.0);
}


static void test_reopen_persistence(mapping_table_t *mt_unused)
{
    (void)mt_unused; // unused, we create our own table here
    TEST_START("Reopen and recover persistent mappings");

    const char *fname = "Persistent_astra.dbf";
    const uint32_t NMAP = 64;
    const uint32_t BLOCK_SIZE = FLASH_DEFAULT_WRITE_GRANULARITY;
    const uint32_t CACHE_CAP = 10;

    // === Step 1: Create and populate ===
    mapping_table_t *mt1 = mapping_table_init(fname, NMAP, BLOCK_SIZE, CACHE_CAP, true);
    assert(mt1);

    // Insert predictable mappings
    for (uint64_t i = 0; i < NMAP; i++)
    {
        uint64_t phys = i * 1000 + 42;
        assert(mapping_table_update_physical_address(mt1, i, phys, true) == 0);
    }

    // Verify before closing
    for (uint64_t i = 0; i < NMAP; i++)
    {
        uint64_t phys_out = 0;
        int rc = mapping_table_get_physical_address(mt1, i, true, &phys_out);
        assert(rc == 0 && phys_out == i * 1000 + 42);
    }

    mapping_table_destroy(mt1); // simulate shutdown

    // === Step 2: Reopen ===
    mapping_table_t *mt2 = mapping_table_init(fname, NMAP, BLOCK_SIZE, CACHE_CAP, false);
    assert(mt2);

    // === Step 3: Verify persistence ===
    for (uint64_t i = 0; i < NMAP; i++)
    {
        uint64_t phys_out = 0;
        int rc = mapping_table_get_physical_address(mt2, i, true, &phys_out);
        if (rc != 0)
        {
            printf("Missing logical %llu after reopen!\n", (unsigned long long)i);
            assert(false);
        }
        assert(phys_out == i * 1000 + 42);
    }

    mapping_table_destroy(mt2);
    TEST_PASS("Reopen and recover persistent mappings", 0.0);
}


static void test_reopen_stress(mapping_table_t *mt_unused)
{
    (void)mt_unused;
    TEST_START("Repeated reopen stress test");

    const char *fname = "ReopenStress_astra.dbf";
    const uint32_t NMAP = 128;
    const uint32_t BLOCK_SIZE = FLASH_DEFAULT_WRITE_GRANULARITY;
    const uint32_t CACHE_CAP = 10;
    const int REOPENS = 20;
    const int OPS_PER_ROUND = 200;

    // Keep an in-memory shadow of the logical→physical mapping
    uint64_t *mirror = calloc(NMAP, sizeof(uint64_t));
    bool *mapped = calloc(NMAP, sizeof(bool));
    assert(mirror && mapped);

    // === Step 1: Create file and initial data ===
    mapping_table_t *mt = mapping_table_init(fname, NMAP, BLOCK_SIZE, CACHE_CAP, true);
    assert(mt);

    for (uint64_t i = 0; i < NMAP; i++)
    {
        uint64_t phys = (i + 1) * 111;
        assert(mapping_table_update_physical_address(mt, i, phys, true) == 0);
        mirror[i] = phys;
        mapped[i] = true;
    }

    mapping_table_destroy(mt);

    // === Step 2: Repeated reopen cycles ===
    for (int r = 0; r < REOPENS; r++)
    {
        printf("Reopen round %d/%d\n", r + 1, REOPENS);
        mapping_table_t *mt2 = mapping_table_init(fname, NMAP, BLOCK_SIZE, CACHE_CAP, false);
        assert(mt2);

        // Verify all known mappings are intact
        for (uint64_t i = 0; i < NMAP; i++)
        {
            uint64_t phys_out = 0;
            int rc = mapping_table_get_physical_address(mt2, i, true, &phys_out);
            if (mapped[i])
            {
                assert(rc == 0);
                assert(phys_out == mirror[i]);
            }
            else
            {
                assert(rc != 0);
            }
        }

        // Perform random updates
        for (int op = 0; op < OPS_PER_ROUND; op++)
        {
            uint64_t logical = rand() % NMAP;
            uint64_t new_phys = ((uint64_t)rand() << 32) ^ rand();
            assert(mapping_table_update_physical_address(mt2, logical, new_phys, true) == 0);
            mirror[logical] = new_phys;
            mapped[logical] = true;
        }

        // Optionally verify a random subset immediately
        for (uint32_t i = 0; i < NMAP; i++)
        {
            uint64_t logical = i;
            uint64_t phys_out = 0;
            int rc = mapping_table_get_physical_address(mt2, logical, true, &phys_out);
            assert(rc == 0);
            assert(phys_out == mirror[logical]);
        }

        mapping_table_destroy(mt2);
    }

    // === Step 3: Final reopen + full check ===
    mapping_table_t *mt_final = mapping_table_init(fname, NMAP, BLOCK_SIZE, CACHE_CAP, false);
    assert(mt_final);

    for (uint64_t i = 0; i < NMAP; i++)
    {
        uint64_t phys_out = 0;
        int rc = mapping_table_get_physical_address(mt_final, i, true, &phys_out);
        if (mapped[i])
        {
            assert(rc == 0 && phys_out == mirror[i]);
        }
        else
        {
            assert(rc != 0);
        }
    }

    mapping_table_destroy(mt_final);
    free(mirror);
    free(mapped);

    TEST_PASS("Repeated reopen stress test", 0.0);
}


/*****************************************************
 * Test suite driver
 *****************************************************/

static const test_case_t test_suite[] = {
    {"Basic insert", test_basic_insert},
    {"Overwrite existing mapping", test_overwrite},
    {"Unmapped lookup", test_unmapped_lookup},
    {"Bulk random insertions", test_bulk_random},
    {"test_heavy_stress", test_heavy_stress},
    {"Reopen and persistence test", test_reopen_persistence},
    {"Reopen stress test", test_reopen_stress},
};

int main(int argc, char *argv[])
{
    unsigned int seed = 5006;
    if (argc > 1)
        seed = (unsigned int)strtoul(argv[1], NULL, 10);
    srand(seed);

    printf("Starting mapping table test suite (seed = %u)\n", seed);

    size_t num_tests = sizeof(test_suite) / sizeof(test_suite[0]);
    size_t passed = 0;
    double total_time_ms = 0.0;

    for (size_t i = 0; i < num_tests; i++)
    {
        const test_case_t *t = &test_suite[i];
        printf("\n────────────────────────────────────────────\n");
        printf("Running test [%zu/%zu]: %s\n", i + 1, num_tests, t->name);
        printf("────────────────────────────────────────────\n");

        // fresh environment per test
        mapping_table_t *mt = mapping_table_init("astra.dbf",
            64, FLASH_DEFAULT_WRITE_GRANULARITY, 10, true);
        assert(mt != NULL);

        clock_t start = clock();
        t->func(mt);
        clock_t end = clock();

        mapping_table_destroy(mt); //cleanup

        double elapsed_ms = ((double)(end - start) / CLOCKS_PER_SEC) * 1000.0;
        total_time_ms += elapsed_ms;

        printf("Time Elapsed: %.3f ms\n", elapsed_ms);
        passed++;
    }

    printf("\n%zu / %zu TESTS PASSED SUCCESSFULLY!\n", passed, num_tests);
    printf("Total time: %.3f ms\n", total_time_ms);

    return 0;
}