#include "graph_defs.h"
#include "balloon.h"
#include "prefetcher.h"

typedef struct scc2_metadata_st {
  unsigned long forward_label;
  unsigned long backward_label;
  volatile unsigned long queue_next;
  char touched;
} scc2_metadata_t;

static volatile unsigned long queue_head = ULONG_MAX;
static volatile unsigned long vertex_position = 0;
static scc2_metadata_t *metadata;
static graph_t * volatile graph;

void prefetcher_random_callback(unsigned long *laf,
				unsigned long laf_size,
				unsigned long ift)
{
  unsigned long current_hoq;
  unsigned long entries = 0;
  /* Fill in inner-loop entries from BFS queue */
  current_hoq = queue_head;
  if(current_hoq != ULONG_MAX) {
    current_hoq = metadata[current_hoq].queue_next;
  }
  while(entries != ift && current_hoq != ULONG_MAX) {
    unsigned long page = graph->vertex_map[current_hoq].offset_edges;
    page = page >> (ASSUME_PAGE_SHIFT + 3); /* offset is in bits ! */
    if(laf[HASH_MODULO(page, laf_size)] != page) {
      laf[HASH_MODULO(page, laf_size)] = page;
      entries++;
    }
    current_hoq = metadata[current_hoq].queue_next;
  }
}


unsigned long prefetcher_sequential_callback(unsigned long* aux_offset)
{
  unsigned long offset = graph->vertex_map[vertex_position].offset_edges;
  return offset >> (ASSUME_PAGE_SHIFT + 3);
}

/* labels each vertex within the same scc with the same label */
unsigned long scc2(graph_t *graph)
{
  unsigned long i = 0;
  unsigned long current_vertex;
  unsigned long queue_tail = ULONG_MAX;
  unsigned long component_count;
  unsigned char different = 1;
  unsigned char firstpass = 1;
  

	while(different != 0){
		different = firstpass == 1 ? 1 : 0;
		component_count = 0;
		for(i = 0; i < graph->vertex_cnt; i++){
		    metadata[i].forward_label = i;
		    metadata[i].touched = 0;
		}
		for(i = 0; i < graph->vertex_cnt; i++){
			if(metadata[i].forward_label == i){
				component_count++;
				if(metadata[i].forward_label!=metadata[i].backward_label){ //Explanation below
					different = 1;
				}
				BFS_PUSH(queue_head, queue_tail, i, metadata);
				while(queue_head != ULONG_MAX) {
					current_vertex = BFS_POP(queue_head, queue_tail, metadata);
					edge_iterator_t iter;
					init_edge_iterator(graph, current_vertex, &iter);
					while(iter_step(graph, &iter) == 0) {
						unsigned long target = iter.neighbour;
						if((!iter.incoming) &&
						metadata[target].touched == 0 &&
						metadata[target].forward_label > metadata[current_vertex].forward_label &&
						(firstpass == 1 || metadata[target].backward_label == metadata[current_vertex].backward_label)) { //Otherwise that edge would have been deleted
							metadata[target].touched = 1;
							metadata[target].forward_label = metadata[current_vertex].forward_label;
							BFS_PUSH(queue_head, queue_tail, target, metadata);
						}
					}
				}
			}
		}
		if(different != 0){
			different = 0;
			component_count = 0;
			for(i = 0; i < graph->vertex_cnt; i++){
				metadata[i].backward_label = i;
			    metadata[i].touched = 0;
			}
			for(i = 0; i < graph->vertex_cnt; i++){
				if(metadata[i].backward_label == i){
					component_count++;
					if(metadata[i].forward_label!=metadata[i].backward_label){ 	//This means there are still edges which should be deleted as
						different = 1;											//i was reached by a forward label but not by its corresponding
					}															//backward label. Note that all vertices traversed in this bfs
					BFS_PUSH(queue_head, queue_tail, i, metadata);				//will have the same forwards and backwards label.
					while(queue_head != ULONG_MAX) {
						current_vertex = BFS_POP(queue_head, queue_tail, metadata);
						edge_iterator_t iter;
						init_edge_iterator(graph, current_vertex, &iter);
						while(iter_step(graph, &iter) == 0) {
							unsigned long target = iter.neighbour;
							if(iter.incoming &&
							metadata[target].touched == 0 &&
							metadata[target].backward_label > metadata[current_vertex].backward_label &&
							metadata[target].forward_label == metadata[current_vertex].forward_label){  //Otherwise that edge would have been deleted
								metadata[target].touched = 1;
								metadata[target].backward_label = metadata[current_vertex].backward_label;
								BFS_PUSH(queue_head, queue_tail, target, metadata);
							}
						}
					}
				}
			}
		}
		firstpass = 0;
	}
	return component_count;
}

int main(int argc, char **argv)
{
  unsigned long time_scc2, time_total, sc_components;
  CLOCK_START(time_total);
  if(argc < 2) {
    fprintf(stderr, "Usage %s graph_name \n", argv[0]);
    exit(-1);
  }
#ifdef PREFETCHER
  bind_master();
  init_prefetcher(prefetcher_random_callback,
		  prefetcher_sequential_callback);
#endif
  graph = open_vertices(argv[1]);
  metadata = (scc2_metadata_t*)
    map_anon_memory(graph->vertex_cnt*sizeof(scc2_metadata_t), "vertex metadata");
  balloon_init();
  balloon_inflate(); /* Simulate semi-em conditions */
  open_cal(graph);
  /* Perhaps mmap /dev/null instead ? */
  memset(metadata, 0, graph->vertex_cnt*sizeof(scc2_metadata_t));
#ifdef PREFETCHER
  launch_prefetch_thread(graph->fd_calist);
#endif
  struct rusage ru_begin;
  getrusage(RUSAGE_SELF, &ru_begin);
  CLOCK_START(time_scc2);
  sc_components = scc2(graph);
  CLOCK_STOP(time_scc2);
  struct rusage ru_end;
  getrusage(RUSAGE_SELF, &ru_end);
#ifdef PREFETCHER
  terminate_prefetch_thread();
  destroy_prefetcher();
#endif
  balloon_deflate();
  munmap(metadata, graph->vertex_cnt*sizeof(scc2_metadata_t));
  close_graph(graph);
  CLOCK_STOP(time_total);
  printf("COMPONENTS %lu\n", sc_components);
  printf("TIME SCC2 %lu\n", time_scc2);
  printf("TIME TOTAL %lu\n", time_total);
  print_rusage_stats(stdout, &ru_begin, &ru_end);
  return 0;
}
