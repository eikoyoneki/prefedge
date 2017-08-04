#ifndef _PREFETCHER2_
#define _PREFETCHER2_

#include "graph_defs.h"
#include <glib.h>
#include <pthread.h>


#define MAX_PAGE_COUNT (uint) 1024
#define MAX_THREAD_COUNT (uint) 64
#define PIPE_SIZE (uint) 4
#define ULL_CHUNK (unsigned char) 128


//NBR_PAGES_t should be 1 if the next node is on the same page
#define MAX_FADVISE_CYCLE 4096000000
#define MSB ((ulong) LONG_MIN)
#define CMSB ((ulong) LONG_MAX) //Check this works
#define SET_MSB(ADDR_t) MSB | ADDR_t
#define UNSET_MSB(ADDR_t) CMSB & ADDR_t
#define TEST_MSB(ADDR_t) MSB & ADDR_t

#define NODE_PAGE(NODE_t,PAGE_t,NBR_PAGES_t) \
  PAGE_t =  graph->index[NODE_t] >> (ASSUME_PAGE_SHIFT + 3); \
  NBR_PAGES_t = (NODE_t + 1 == graph->vertex_cnt) ? 1 : (graph->index[NODE_t+1] >> (ASSUME_PAGE_SHIFT + 3)) - PAGE_t + 1

#define NODE_PAGE_AUX(NODE_t,PAGE_t,NBR_PAGES_t) \
  PAGE_t =  graph->index_aux[NODE_t] >> (ASSUME_PAGE_SHIFT ); \
  NBR_PAGES_t = (NODE_t + 1 == graph->vertex_cnt) ? 1 :(graph->index_aux[NODE_t+1] >> ASSUME_PAGE_SHIFT) - PAGE_t + 1; \
  PAGE_t = SET_MSB(PAGE_t)



#define RUNNING 1
#define TERMINATE 2
#define MAX_NODES_PREFETCHER


typedef struct heap_element_st{
  int priority;
  unsigned long node;
} heap_element_t;

typedef struct LRU_element_st{  //Struct to represent the pages of the nodes in the top_heap
  unsigned long page_addr;  //The address of the page
  unsigned int nbr_pages;   //The number of pages that need to be sequentially fetched
                             //non zero only if the last node of the page is in the top heap
  unsigned int nbr_nodes;  //The number of nodes on this page in the top_heap
} LRU_element_t;


struct LRU_ULL_st;
typedef struct LRU_ULL_st LRU_ULL_t;

struct LRU_ULL_st{ //An unrolled linked list with LRU elements inside
  LRU_ULL_t * next;           //The next sequence of LRU elements in the linked list
  unsigned char nbr_elements; //The number of LRU elements in this sequence
  LRU_element_t elements[ULL_CHUNK];   //The sequence of LRU elements
  unsigned int timestamp; //The number of fadvises that had been issued last time this was processed
};

typedef struct prefetch_pipe_st{
  unsigned int thread_id;
  volatile heap_element_t pipe_array[PIPE_SIZE];
  volatile int read_index;
  volatile int write_index;
} prefetch_pipe_t;


void iter();
void clean_ull(LRU_ULL_t * ull);
void next_ull();
void bubble_down_top(uint pos);
void bubble_up_top(uint pos);
void bubble_down_bottom(uint pos);
void bubble_up_bottom(uint pos);

void bottom_remove(uint index);
void top_remove(uint index);

void bottom_change_priority(uint index, int priority);
void top_change_priority(uint index, int priority);
void bottom_pop();
void top_pop();
void top_pop_without_balance();

void bottom_insert(heap_element_t * he);
void top_insert(heap_element_t * he);
void process_pipe(unsigned int thread);
char heap_test(heap_element_t * he);
void iterator(gpointer key, gpointer value, gpointer user_data);
void lru_remove(ulong node);
void lru_add(ulong node);
void split_ull();
void fadvise_wn(LRU_element_t* e);
void fadvise_dn(LRU_element_t* e);
void balance_heaps();
int lru_page_count();

extern void init_prefetcher(csr_t * g);
extern void prefetcher_set_aux_fd();
extern void launch_prefetch_thread();
extern void terminate_prefetch_thread();
extern void destroy_prefetcher();
extern void print_prefetcher();

extern void test();

extern unsigned int register_thread();
extern void deregister_thread();

#endif
