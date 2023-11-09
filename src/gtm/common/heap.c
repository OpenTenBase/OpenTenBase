/*
 * Copyright (c) 2023 THL A29 Limited, a Tencent company.
 *
 * This source code file is licensed under the BSD 3-Clause License,
 * you may obtain a copy of the License at http://opensource.org/license/bsd-3-clause/
 */
/**
 * This file defines the methods declared in heap.h
 * These are used to create and manipulate a heap
 * data structure.
 */

#include <unistd.h>
#include <sys/mman.h>
#include <assert.h>
#include <strings.h>
#include <string.h>
#include "gtm/heap.h"

// Helpful Macros
#define LEFT_CHILD(i)   ((i<<1)+1)
#define RIGHT_CHILD(i)  ((i<<1)+2)
#define PARENT_ENTRY(i) ((i-1)>>1)
#define SWAP_ENTRIES(parent,child)  { \
                                      void* temp = parent->key; \
                                      parent->key = child->key;          \
                                      child->key = temp;                 \
                                      temp = parent->value;              \
                                      parent->value = child->value;      \
                                      child->value = temp;               \
                                    }

#define GET_ENTRY(index,table) ((heap_entry*)(table+index))

static int compare_int_keys(register void* key1, register void* key2);


/**
 * Stores the number of heap_entry structures
 * we can fit into a single page of memory.
 *
 * This is determined by the page size, so we
 * need to determine this at run time.
 */
static int ENTRIES_PER_PAGE = 0;

/**
 * Stores the number of bytes in a single
 * page of memory.
 */
static int PAGE_SIZE = 0;

// Helper function to map a number of pages into memory
// Returns NULL on error, otherwise returns a pointer to the
// first page.
static void* map_in_pages(int page_count) {

    void* addr = NULL;
    // Check everything
    assert(page_count > 0);

    // Call mmmap to get the pages

    addr = mmap(NULL, page_count*PAGE_SIZE, PROT_READ|PROT_WRITE, MAP_ANON|MAP_PRIVATE, -1, 0);

    if (addr == MAP_FAILED)
        return NULL;
    else {
        // Clear the memory
        bzero(addr,page_count*PAGE_SIZE);
        
        // Return the address 
        return addr;
    }
}


// Helper function to map a number of pages out of memory
static void map_out_pages(void* addr, int page_count) {

    int result = 0 ;
    // Check everything
    assert(addr != NULL);
    assert(page_count > 0);

    // Call munmap to get rid of the pages
    result = munmap(addr, page_count*PAGE_SIZE);

    // The result should be 0
    assert(result == 0);
}

// This is a comparison function that treats keys as signed ints
static int compare_int_keys(register void* key1, register void* key2) {
    // Cast them as int* and read them in
    register int key1_v = *((int*)key1);
    register int key2_v = *((int*)key2);

    // Perform the comparison
    if (key1_v < key2_v)
        return -1;
    else if (key1_v == key2_v)
        return 0;
    else
        return 1;
}


// Creates a new heap
void heap_create(heap* h, int initial_size, int (*comp_func)(void*,void*)) {
    // Check if we need to setup our globals
    if (PAGE_SIZE == 0) {
        // Get the page size
        PAGE_SIZE = getpagesize();

        // Calculate the max entries
        ENTRIES_PER_PAGE = PAGE_SIZE / sizeof(heap_entry);
    }
    
    // Check that initial size is greater than 0, else set it to ENTRIES_PER_PAGE
    if (initial_size <= 0)
        initial_size = ENTRIES_PER_PAGE;

    // If the comp_func is null, treat the keys as signed ints
    if (comp_func == NULL)
        comp_func = compare_int_keys;


    // Store the compare function
    h->compare_func = comp_func;

    // Set active entries to 0
    h->active_entries = 0;

    // Determine how many pages of entries we need
    h->allocated_pages = initial_size / ENTRIES_PER_PAGE + ((initial_size % ENTRIES_PER_PAGE > 0) ? 1 : 0);
    h->minimum_pages = h->allocated_pages;

    // Allocate the table
    h->table = (void*)map_in_pages(h->allocated_pages);
}


// Cleanup a heap
void heap_destroy(heap* h) {
    // Check that h is not null
    assert(h != NULL);

    // Map out the table
    map_out_pages(h->table, h->allocated_pages);

    // Clear everything
    h->active_entries = 0;
    h->allocated_pages = 0;
    h->table = NULL;
}


// Gets the size of the heap
int heap_size(heap* h) {
    // Return the active entries
    return h->active_entries;
}


// Gets the minimum element
int heap_min(heap* h, void** key, void** value) {
    heap_entry* root  = NULL;

    // Check the number of elements, abort if 0
    if (h->active_entries == 0)
        return 0;

    // Get the 0th element
    root = GET_ENTRY(0, h->table);

    // Set the key and value
    *key = root->key;
    *value = root->value;

    // Success
    return 1;
}


// Insert a new element
void heap_insert(heap *h, void* key, void* value) {
    int max_entries = 0 ;
    int (*cmp_func)(void*,void*) = NULL;
    heap_entry* table  = NULL;
    heap_entry* current = NULL;
    int current_index;
    // Loop variables
    int parent_index;
    heap_entry *parent;

    // Check if this heap is not destoyed
    assert(h->table != NULL);

    // Check if we have room
    max_entries =  h->allocated_pages * ENTRIES_PER_PAGE;
    if (h->active_entries + 1 > max_entries) {
        // Get the new number of entries we need
        int new_size = h->allocated_pages * 2;

        // Map in a new table
        heap_entry* new_table = map_in_pages(new_size);

        // Copy the old entries, copy the entire pages
        memcpy(new_table, h->table, h->allocated_pages*PAGE_SIZE);
        
        // Cleanup the old table
        map_out_pages(h->table, h->allocated_pages);

        // Switch to the new table
        h->table = new_table;
        h->allocated_pages = new_size;
    }
    
    // Store the comparison function
    cmp_func = h->compare_func;

    // Store the table address
    table = h->table;

    // Get the current index
    current_index = h->active_entries;
    current = GET_ENTRY(current_index, table);

    // While we can, keep swapping with our parent
    while (current_index > 0) {
        // Get the parent index
        parent_index = PARENT_ENTRY(current_index);

        // Get the parent entry
        parent = GET_ENTRY(parent_index, table);
       
        // Compare the keys, and swap if we need to 
        if (cmp_func(key, parent->key) < 0) {
            // Move the parent down
            current->key = parent->key;
            current->value = parent->value;

            // Move our reference
            current_index = parent_index;
            current = parent;

        // We are done swapping
        }   else
            break;
    }

    // Insert at the current index
    current->key = key;
    current->value = value; 

    // Increase the number of active entries
    h->active_entries++;
}


// Deletes the minimum entry in the heap
int heap_delmin(heap* h, void** key, void** value) {

    heap_entry* table = NULL;
    heap_entry* current = NULL;
    int current_index = 0;
    int entries = 0;
    int used_pages = 0;
    // Store the left index
    int left_child_index;

    // Check there is a minimum
    if (h->active_entries == 0)
        return 0;

    // Load in the map table
    table = h->table;

    // Get the root element
    current = GET_ENTRY(current_index, table);

    // Store the outputs
    *key = current->key;
    *value = current->value;

    // Reduce the number of active entries
    h->active_entries--;

    // Get the active entries
    entries = h->active_entries;
   
    // If there are any other nodes, we may need to move them up
    if (h->active_entries > 0) {
        // Move the last element to the root
        heap_entry* last = NULL ;
        // Loop variables
        heap_entry* left_child;
        heap_entry* right_child;
        int (*cmp_func)(void*,void*) = NULL;

        last = GET_ENTRY(entries,table);
        current->key = last->key;
        current->value = last->value;

        // Load the comparison function
        cmp_func = h->compare_func;


        while (left_child_index = LEFT_CHILD(current_index), left_child_index < entries) {
            // Load the left child
            left_child = GET_ENTRY(left_child_index, table);

            // We have a left + right child
            if (left_child_index+1 < entries) {
                // Load the right child
                right_child = GET_ENTRY((left_child_index+1), table);

                // Find the smaller child
                if (cmp_func(left_child->key, right_child->key) <= 0) {

                    // Swap with the left if it is smaller
                    if (cmp_func(current->key, left_child->key) == 1) {
                        SWAP_ENTRIES(current,left_child);
                        current_index = left_child_index;
                        current = left_child;

                    // Otherwise, the current is smaller
                    } else
                        break;

                // Right child is smaller
                } else {

                    // Swap with the right if it is smaller
                    if (cmp_func(current->key, right_child->key) == 1) {
                        SWAP_ENTRIES(current,right_child);
                        current_index = left_child_index+1;
                        current = right_child;

                    // Current is smaller
                    } else
                        break;

                }


            // We only have a left child, only do something if the left is smaller
            } else if (cmp_func(current->key, left_child->key) == 1) {
                SWAP_ENTRIES(current,left_child);
                current_index = left_child_index;
                current = left_child;

            // Done otherwise
            }  else
                break;

        }
    } 

    // Check if we should release a page of memory
    used_pages = entries / ENTRIES_PER_PAGE + ((entries % ENTRIES_PER_PAGE > 0) ? 1 : 0);

    // Allow one empty page, but not two
    if (h->allocated_pages / 2 > used_pages + 1 && h->allocated_pages / 2 >= h->minimum_pages) {
        // Get the new number of entries we need
        int new_size = h->allocated_pages / 2;

        // Map in a new table
        heap_entry* new_table = map_in_pages(new_size);

        // Copy the old entries, copy the entire pages
        memcpy(new_table, h->table, used_pages*PAGE_SIZE);
        
        // Cleanup the old table
        map_out_pages(h->table, h->allocated_pages);

        // Switch to the new table
        h->table = new_table;
        h->allocated_pages = new_size;
    }

    // Success
    return 1;
}


// Allows a user to iterate over all entries, e.g. to free() the memory
void heap_foreach(heap* h, void (*func)(void*,void*)) {
    // Store the current index and max index
    int index = 0;
    int entries = h->active_entries;

    heap_entry* entry;
    heap_entry* table = h->table;

    for (;index<entries;index++) {
        // Get the entry
        entry = GET_ENTRY(index,table);

        // Call the user function
        func(entry->key, entry->value);
    }
}


