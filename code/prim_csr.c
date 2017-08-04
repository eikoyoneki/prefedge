#include "graph_defs.h"
#include "prefetcher.h"

typedef struct prim_metadata_st {
  char touched; /* 0 == unseen, 1 == seen, 2 == done */
  unsigned long heap_index;
  unsigned long root;
} prim_metadata_t;

#define HEAP_OFFSET (unsigned long)&(((prim_metadata_t *)0)->heap_index)
#define PRIM_CONTAINER_OF(_indexp) (prim_metadata_t *)(((unsigned long)(_indexp)) - HEAP_OFFSET)

static csr_t * volatile graph;
static volatile unsigned long vertex_position = 0;
static heap_t *heap;
static prim_metadata_t *metadata;


void prefetcher_random_callback(unsigned long *laf,
				unsigned long laf_size,
				unsigned long ift)
{
  unsigned long current_boh, i;
  unsigned long entries = 0;
  current_boh = heap->heap_bottom;
  /* No entries in heap ? :( */
  if(current_boh == ULONG_MAX) {
    return;
  }
  /* Fill in inner-loop entries from top of heap */
  i = 1; /* Skip top element */
  while(entries != ift && i <= current_boh) {
    unsigned long *indexp = heap_peek(heap, i);
    if(indexp != NULL) { /* could be null due to race with heap_add */
      prim_metadata_t * current_metadata =
	PRIM_CONTAINER_OF(indexp);
      unsigned long current_vertex = current_metadata - &metadata[0];
      unsigned long page = graph->index[current_vertex];
      unsigned long end = graph->index[current_vertex+1];
      unsigned long aux_page = graph->index_aux[current_vertex];
      unsigned long aux_end = graph->index_aux[current_vertex+1];

      aux_page = aux_page >> ASSUME_PAGE_SHIFT; 
      aux_end = aux_end >> ASSUME_PAGE_SHIFT;
      page = page >> (ASSUME_PAGE_SHIFT + 3); /* offset is in bits ! */
      end = end >> (ASSUME_PAGE_SHIFT + 3); 
      laf[entries] = page;
      if (end > page) laf[entries+(2*laf_size)] = end - page;
      laf[entries+laf_size] = aux_page;
      if (aux_end > aux_page) laf[entries+(3*laf_size)] = aux_end - aux_page;
      entries++;
      
    }
    i++;
  }
}

void prim(unsigned long start_node,
	  unsigned char *rew)
{
  unsigned long i;
  unsigned long alist_entries_seen = 0;
  unsigned long vertices_covered = 0;
  i = start_node;
  do {
    if(metadata[i].touched == 0) {
      vertex_position = i;
      metadata[i].touched = 1;
      heap_add(heap, 0.0, &metadata[i].heap_index);
      while(!heap_is_empty(heap)) {
        prim_metadata_t * current_metadata =
          PRIM_CONTAINER_OF(heap_get_min_index(heap));
        unsigned long current_vertex = current_metadata - &metadata[0];
        csr_edge_iterator_t iter;
        csr_init_edge_iterator(graph, current_vertex, &iter);
        unsigned long edge_count = 0;
        double current_vertex_cost = heap_get_min_key(heap);
        heap_remove_min(heap);
        vertices_covered++;
        current_metadata->touched = 2; /* off the heap */
        current_metadata->root = i;
        double * rew_costs = (double *)&rew[graph->index_aux[current_vertex]];
        while(csr_iter_step(graph, &iter) == 0) {
	  double edge_cost = rew_costs[edge_count++];
          if(!iter.incoming) {
            unsigned long target = iter.neighbour;
            if(metadata[target].touched == 0) {
              metadata[target].touched = 1;
              heap_add(heap, edge_cost, &metadata[target].heap_index);
            }
            else if(metadata[target].touched == 1) {
              if(heap_get_key(heap, metadata[target].heap_index) >(edge_cost)) {
                heap_reduce_key(heap, metadata[target].heap_index, (edge_cost));
              }
            }
          }
          alist_entries_seen++;
        }
      }
    }
#ifdef TRUNC_ALG
    return;
#endif
    i = i + 1;
    if(i >= graph->vertex_cnt) {
      i = 0;
    }
  } while(i != start_node);
  assert(alist_entries_seen == graph->alist_entries);
  assert(vertices_covered == graph->vertex_cnt);
}

int main(int argc, char **argv)
{
  unsigned char string_buffer[1024];
  unsigned long clock_total, clock_prim;
  unsigned long keep_compiler_happy;
  CLOCK_START(clock_total);
  if(argc < 3) {
    fprintf(stderr, "Usage %s graph root_id", argv[0]);
    exit(-1);
  }
#ifdef PREFETCHER
  bind_master();
  init_prefetcher(prefetcher_random_callback, 
		  NULL); //prefetcher_sequential_callback);
#endif
  graph = open_csr_aux(argv[1]);
  heap = allocate_heap(graph->vertex_cnt);
  metadata = (prim_metadata_t *)
    map_anon_memory(graph->vertex_cnt*sizeof(prim_metadata_t), "prim metadata");
  memset(metadata, 0, graph->vertex_cnt*sizeof(prim_metadata_t));
  
  register_mlocked_memory(graph->vertex_cnt*sizeof(unsigned long));
  unsigned long root = atol(argv[2]);
  assert(root < graph->vertex_cnt);

#ifdef PREFETCHER
#ifdef AUX_PREFETCH
  prefetcher_set_aux_fd(graph->fd_aux);
#endif
  launch_prefetch_thread(graph->fd_calist);
#endif
  print_mlocked_memory();
  struct rusage ru_begin;
  getrusage(RUSAGE_SELF, &ru_begin);
  CLOCK_START(clock_prim);
  prim(root, graph->aux);
  CLOCK_STOP(clock_prim);
  struct rusage ru_end;
  getrusage(RUSAGE_SELF, &ru_end);
#ifdef PREFETCHER
  terminate_prefetch_thread();
  destroy_prefetcher();
#endif
  munmap(metadata, graph->vertex_cnt*sizeof(prim_metadata_t));
  destroy_heap(heap);
  close_csr(graph);
  CLOCK_STOP(clock_total);
  printf("TIME PRIM %lu\n", clock_prim);
  printf("TIME TOTAL %lu\n", clock_total);
  print_rusage_stats(stdout, &ru_begin, &ru_end);
  return 0;
}
