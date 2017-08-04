#include "graph_defs.h"
#include "prefetcher2.h"

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
unsigned int thread_id;


unsigned long total_queue_demands = 0;
unsigned long queue_above_threshold = 0;
unsigned long queue_length = 0;

unsigned char* cmap;
unsigned long cmap_size;

#define max_count_nodes 50000
void sssp(unsigned long start_node,
	  unsigned char *rew)
{
  unsigned long i;
  unsigned long alist_entries_seen = 0;
  unsigned long vertices_covered = 0;
  i = start_node;
  unsigned long count_nodes = 0;
  do {
    if(metadata[i].touched == 0) {
      metadata[i].touched = 1;
      vertex_position = i;
      heap_add(heap, 0.0, &metadata[i].heap_index);
      queue_length = 1;
      while(!heap_is_empty(heap) && vertices_covered < max_count_nodes) {
    	  count_nodes++;
    	  sssp_metadata_t * current_metadata =
          SSSP_CONTAINER_OF(heap_get_min_index(heap));
        unsigned long current_vertex = current_metadata - &metadata[0];
#ifdef PREFETCHER
        write_pipe(thread_id, current_vertex, INT_MIN);
#endif
        csr_edge_iterator_t iter;
        double * rew_costs =
          (double *)&graph->aux[graph->index_aux[current_vertex]];

//#ifndef NDEBUG
        /*mincore(&graph->calist_map[BYTE_INDEX(graph->index[current_vertex])], 1, cmap);
        if(cmap[0]){
            //printf("Vertex %lu edge list currently in memory\n", current_vertex);
        }else{
            printf("Vertex %lu edge list NOT currently in memory\n", current_vertex);
        }
        mincore(rew_costs, 1, cmap);
        if(cmap[0]){
            //printf("Vertex %lu edge weights currently in memory\n", current_vertex);
        }else{
            printf("Vertex %lu edge weights NOT currently in memory\n", current_vertex);
        }*/
        unsigned long page;
        page = graph->index[current_vertex] >> (ASSUME_PAGE_SHIFT + 3);
        page = page * ASSUME_PAGE_SIZE;
        /*if(fadvise_test(page)){
            printf("On vertex %lu\n",current_vertex);
        }*/
//#endif
        csr_init_edge_iterator(graph, current_vertex, &iter);
        unsigned long edge_count = 0;
        double current_vertex_cost = heap_get_min_key(heap);
        total_queue_demands++;
        if(queue_length >= RASP_THRESHOLD) {
          queue_above_threshold++;
        }
        unsigned long new_node = heap_remove_min(heap);
#ifdef PREFETCHER
        if(heap->heap_bottom != ULONG_MAX && new_node != 0){
          unsigned long v = SSSP_CONTAINER_OF(heap->heap_entries[new_node].indexp)- &metadata[0];
          int p = (int)((-1.0) * (heap->heap_entries[new_node].key)) ;
          write_pipe(thread_id, v , p);
        }
#endif
        queue_length--;
        vertices_covered++;
        current_metadata->touched = 2; /* off the heap */
#ifndef NDEBUG
          current_metadata->reach_cost = current_vertex_cost;
        current_metadata->root = i;
#endif
//#ifndef NDEBUG
//#endif
        while(csr_iter_step(graph, &iter) == 0) {
          double edge_cost = rew_costs[edge_count++];
          if(!iter.incoming) {
            unsigned long target = iter.neighbour;
            if(metadata[target].touched == 0) {
              metadata[target].touched = 1;
              heap_add(heap,
	               current_vertex_cost + edge_cost,
	               &metadata[target].heap_index);
#ifdef PREFETCHER
              if(metadata[target].heap_index <32){
               // printf("Node: %lu, Priority: %i\n", he.node, he.priority);
                write_pipe(thread_id, target, (int)((-1.0) * (current_vertex_cost + edge_cost)));
              }
#endif
              queue_length++;
            }
            else if(metadata[target].touched == 1) {
              if(heap_get_key(heap, metadata[target].heap_index) >
	         (current_vertex_cost + edge_cost)) {
	        heap_reduce_key(heap,
			        metadata[target].heap_index,
			        (current_vertex_cost + edge_cost));
#ifdef PREFETCHER
                if(metadata[target].heap_index <32){
                 // printf("Node: %lu, Priority: %i\n", he.node, he.priority);
                  write_pipe(thread_id, target, (int)((-1.0) * (current_vertex_cost + edge_cost)));
                }
#endif
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
  } while(i != start_node && vertices_covered < max_count_nodes);
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
  graph = open_csr_aux(argv[1]);
#ifdef PREFETCHER
  init_prefetcher(graph);
  launch_prefetch_thread();
  thread_id = register_thread();
  //terminate_prefetch_thread();
#endif
  heap = allocate_heap(graph->vertex_cnt);
  metadata = (sssp_metadata_t *)
    map_anon_memory(graph->vertex_cnt*sizeof(sssp_metadata_t), "sssp metadata");
  memset(metadata, 0, graph->vertex_cnt*sizeof(sssp_metadata_t));
  print_mlocked_memory();
  unsigned long root = atol(argv[2]);
  assert(root < graph->vertex_cnt);
  cmap_size = 1;
  cmap = (unsigned char*) malloc(cmap_size);



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
  printf("TIME SSSP----- %lu\n", clock_sssp);
  printf("TIME TOTAL %lu\n", clock_total);
  print_rusage_stats(stdout, &ru_begin, &ru_end);
  printf("F_THRESHOLD %f\n", ((double)queue_above_threshold)/total_queue_demands);
  return 0;
}
