#include "graph_defs.h"
#include "prefetcher.h"

typedef struct pr_metadata_st {
  double p_old;
  double p_new;
  unsigned long degree;
} pr_metadata_t;

static volatile unsigned long vertex_position = 0;
static csr_t * volatile graph;

unsigned long prefetcher_sequential_callback(unsigned long *auxp)
{
  unsigned long offset = graph->index[vertex_position];
  return offset >> (ASSUME_PAGE_SHIFT + 3);
}


double pagerank(pr_metadata_t *metadata,
	    unsigned long iter_count)
{
  unsigned long i, ic;
  unsigned long din=0,dout=0,cross=0;
  i = 0;
  for(i=0; i<graph->vertex_cnt; i++) {
    	vertex_position = i;
	metadata[i].p_old = 1.0 / graph->vertex_cnt;
	metadata[i].p_new = 0.0;
	metadata[i].degree = 0;
	csr_edge_iterator_t iter;
	csr_init_edge_iterator(graph, i, &iter);
	while(csr_iter_step(graph, &iter) == 0) {
		if(!iter.incoming) {
			metadata[i].degree++;
		}
	}
  }
  ic = 0;
  for(ic = 0; ic < iter_count; ic++){
	for(i=0; i<graph->vertex_cnt; i++) {
      	        vertex_position = i;
	        metadata[i].p_new += 0.15 / (double) graph->vertex_cnt; 
		csr_edge_iterator_t iter;
		csr_init_edge_iterator(graph, i, &iter);
		while(csr_iter_step(graph, &iter) == 0) {
			if(!iter.incoming) {
				metadata[iter.neighbour].p_new += 0.85 * metadata[i].p_old / (double) metadata[i].degree;
			}
		}
	  }
	  for(i=0; i<graph->vertex_cnt; i++){
		metadata[i].p_old = metadata[i].p_new;
		metadata[i].p_new = 0.0;
	  }
  }
}

int main(int argc, char **argv)
{
  unsigned long time_pr, time_total;
  CLOCK_START(time_total);
  if(argc < 3) {
    fprintf(stderr, "Usage %s graph_name PageRank iterations\n", argv[0]);
    exit(-1);
  }
#ifdef PREFETCHER
  bind_master();
  init_prefetcher(NULL, prefetcher_sequential_callback);
#endif
  graph = open_csr(argv[1]);
  unsigned long iter = atol(argv[2]);
  pr_metadata_t * pr_metadata = (pr_metadata_t*)
    map_anon_memory(graph->vertex_cnt*sizeof(pr_metadata_t), "vertex metadata");
  /* Perhaps mmap /dev/null instead ? */
//  balloon_inflate(); /* Simulate semi-em conditions */
//  open_csr(graph);
#ifdef PREFETCHER
  launch_prefetch_thread(graph->fd_calist);
#endif
  struct rusage ru_begin;
  getrusage(RUSAGE_SELF, &ru_begin);
  CLOCK_START(time_pr);
  pagerank(pr_metadata, iter);  
  CLOCK_STOP(time_pr);
  struct rusage ru_end;
  getrusage(RUSAGE_SELF, &ru_end);
#ifdef PREFETCHER
  terminate_prefetch_thread();
  destroy_prefetcher();
#endif
  munmap(pr_metadata, graph->vertex_cnt*sizeof(pr_metadata_t));
  close_csr(graph);
  CLOCK_STOP(time_total);
  printf("TIME PAGERANK %lu\n", time_pr);
  printf("TIME TOTAL %lu\n", time_total);
  print_rusage_stats(stdout, &ru_begin, &ru_end);
  return 0;
}
