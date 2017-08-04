#include "graph_defs.h"
#include "prefetcher.h"


typedef struct sssp_metadata_st {
  char touched; /* 0 == unseen, 1 == seen, 2 == done */
  unsigned long heap_index;
  #ifndef NDEBUG
  double reach_cost;
  unsigned long root;
  #endif
} sssp_metadata_t;

#define HEAP_OFFSET (unsigned long)&(((sssp_metadata_t *)0)->heap_index)
#define SSSP_CONTAINER_OF(_indexp) (sssp_metadata_t *)(((unsigned long)(_indexp)) - HEAP_OFFSET)

static csr_t * volatile graph;
static volatile unsigned long vertex_position = 0;
static heap_t *heap;
static sssp_metadata_t *metadata;

unsigned long prefetcher_sequential_callback(unsigned long *auxp)
{
  unsigned long offset = graph->index[vertex_position];
  *auxp = (graph->index_aux[vertex_position] >> ASSUME_PAGE_SHIFT);
  return offset >> (ASSUME_PAGE_SHIFT + 3);
}

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
      sssp_metadata_t * current_metadata =
	SSSP_CONTAINER_OF(indexp);
      unsigned long current_vertex = current_metadata - &metadata[0];
      unsigned long page = graph->index[current_vertex];
      unsigned long end = graph->index[current_vertex+1];
      page = page >> (ASSUME_PAGE_SHIFT + 3); /* offset is in bits ! */
      end = end >> (ASSUME_PAGE_SHIFT + 3);
      unsigned long aux_page = graph->index_aux[current_vertex];
      unsigned long aux_end = graph->index_aux[current_vertex+1];
      aux_page = aux_page >> ASSUME_PAGE_SHIFT; /* offset is in bytes ! */
      aux_end = aux_end >> ASSUME_PAGE_SHIFT;
//      fprintf(stderr, "%ld %ld %ld\n", aux_page, aux_end, (rew_index[current_vertex+2] >> ASSUME_PAGE_SHIFT));
 //       if (aux_end - aux_page > 32) aux_end = aux_page;

//      if(laf[HASH_MODULO(page, laf_size)] != page) {
//	for (; ((page <= end) || (aux_page <=aux_end)); page++, aux_page++) {
//	laf[HASH_MODULO(page, laf_size)] = page;
	  laf[entries] = page;
	  if (end > page) laf[entries+(2*laf_size)] = end - page;
//	laf[HASH_MODULO(page, laf_size) + laf_size] = aux_page;
	  laf[entries+laf_size] = aux_page;
	  if (aux_end > aux_page) laf[entries+(3*laf_size)] = aux_end - aux_page;
          entries++;
//	  if (entries == ift) break;
//        }
//      }
    }
    i++;
  }
}

unsigned long total_queue_demands = 0;
unsigned long queue_above_threshold = 0;
unsigned long queue_length = 0;

#define max_verts 50000

void sssp(unsigned long start_node,
	  unsigned char *rew)
{
  unsigned long i;
  unsigned long alist_entries_seen = 0;
  unsigned long vertices_covered = 0;
  i = start_node;
  do {
    if(metadata[i].touched == 0) {
      metadata[i].touched = 1;
      vertex_position = i;
      heap_add(heap, 0.0, &metadata[i].heap_index);
      queue_length = 1;
      while(!heap_is_empty(heap) && vertices_covered < max_verts) {
	sssp_metadata_t * current_metadata =
	  SSSP_CONTAINER_OF(heap_get_min_index(heap));
	unsigned long current_vertex = current_metadata - &metadata[0];
	csr_edge_iterator_t iter;
	csr_init_edge_iterator(graph, current_vertex, &iter);
	unsigned long edge_count = 0;
	double current_vertex_cost = heap_get_min_key(heap);
	total_queue_demands++;
	if(queue_length >= RASP_THRESHOLD) {
	  queue_above_threshold++;
	}
	heap_remove_min(heap);
	queue_length--;
	vertices_covered++;
	current_metadata->touched = 2; /* off the heap */
#ifndef NDEBUG
	current_metadata->reach_cost = current_vertex_cost;
	current_metadata->root = i;
#endif
	double * rew_costs =
	  (double *)&rew[graph->index_aux[current_vertex]];
	while(csr_iter_step(graph, &iter) == 0) {
	  double edge_cost = rew_costs[edge_count++];
	  if(!iter.incoming) {
	    unsigned long target = iter.neighbour;
	    if(metadata[target].touched == 0) {
	      metadata[target].touched = 1;
	      heap_add(heap,
		       current_vertex_cost + edge_cost,
		       &metadata[target].heap_index);
	      queue_length++;
	    }
	    else if(metadata[target].touched == 1) {
	      if(heap_get_key(heap, metadata[target].heap_index) >
		 (current_vertex_cost + edge_cost)) {
		heap_reduce_key(heap,
				metadata[target].heap_index,
				(current_vertex_cost + edge_cost));
	      }
	    }
	    else {
	      assert((metadata[target].root != i) ||
		     (metadata[target].reach_cost <=
		      (current_vertex_cost + edge_cost)));
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
  } while(i != start_node && vertices_covered < max_verts);
  assert(alist_entries_seen == graph->alist_entries);
  assert(vertices_covered == graph->vertex_cnt);
}

int main(int argc, char **argv)
{
  unsigned long clock_total, clock_sssp;
  CLOCK_START(clock_total);
  if(argc < 3) {
    fprintf(stderr, "Usage %s graph root_id", argv[0]);
    exit(-1);
  }
#ifdef PREFETCHER
  bind_master();
  init_prefetcher(prefetcher_random_callback, NULL);
		//  prefetcher_sequential_callback);
#endif
  graph = open_csr_aux(argv[1]);
  heap = allocate_heap(graph->vertex_cnt);
  metadata = (sssp_metadata_t *)
    map_anon_memory(graph->vertex_cnt*sizeof(sssp_metadata_t), "sssp metadata");
  memset(metadata, 0, graph->vertex_cnt*sizeof(sssp_metadata_t));

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
  CLOCK_START(clock_sssp);
  sssp(root, graph->aux);
  CLOCK_STOP(clock_sssp);
  struct rusage ru_end;
  getrusage(RUSAGE_SELF, &ru_end);
#ifdef PREFETCHER
  terminate_prefetch_thread();
  destroy_prefetcher();
#endif
  close_csr(graph);
  CLOCK_STOP(clock_total);
  printf("TIME SSSP %lu\n", clock_sssp);
  printf("TIME TOTAL %lu\n", clock_total);
  print_rusage_stats(stdout, &ru_begin, &ru_end);
  printf("F_THRESHOLD %f\n", ((double)queue_above_threshold)/total_queue_demands);
  return 0;
}
