#include "graph_defs.h"
#include "balloon.h"
#include "prefetcher.h"

typedef struct cond_metadata_st {
  unsigned long part;
} cond_metadata_t;

static volatile unsigned long vertex_position = 0;
static graph_t * volatile graph;

unsigned long prefetcher_sequential_callback(unsigned long *auxp)
{
  unsigned long offset = graph->vertex_map[vertex_position].offset_edges;
  return offset >> (ASSUME_PAGE_SHIFT + 3);
}


double cond(cond_metadata_t *metadata,
	    unsigned long part)
{
  unsigned long i;
  unsigned long din=0,dout=0,cross=0;
  i = 0;
  for(i=0; i<graph->vertex_cnt; i++) {
    if(metadata[i].part == part){
      vertex_position = i;
      edge_iterator_t iter;
      init_edge_iterator(graph, i, &iter);
      while(iter_step(graph, &iter) == 0) {
          if(metadata[iter.neighbour].part != part){
            cross++;
          }
	  din++;
      }
    }else{
      edge_iterator_t iter;
      init_edge_iterator(graph, i, &iter);
      while(iter_step(graph, &iter) == 0) {
	dout++;
      }
    }
  }
  double m = din<dout ? din : dout;
  if(m == 0.0){
    if(cross==0){
      return 0.0;
    }else{
      return 1.0/0.0;
    }
  }
  return((double) cross / m);
}

int main(int argc, char **argv)
{
  unsigned long time_cond, time_total;
  CLOCK_START(time_total);
  if(argc < 3) {
    fprintf(stderr, "Usage %s graph_name number of partitions\n", argv[0]);
    exit(-1);
  }
#ifdef PREFETCHER
  bind_master();
  init_prefetcher(NULL, prefetcher_sequential_callback);
#endif
  graph = open_vertices(argv[1]);
  unsigned long parts = atol(argv[2]);
  assert(parts < graph->vertex_cnt);
  cond_metadata_t * cond_metadata = (cond_metadata_t*)
    map_anon_memory(graph->vertex_cnt*sizeof(cond_metadata_t), "vertex metadata");
  /* Perhaps mmap /dev/null instead ? */
  unsigned long i;
  for(i=0; i< graph->vertex_cnt; i++){
    cond_metadata[i].part = i % parts;
  }
  balloon_init();
  balloon_inflate(); /* Simulate semi-em conditions */
  open_cal(graph);
#ifdef PREFETCHER
  launch_prefetch_thread(graph->fd_calist);
#endif
  struct rusage ru_begin;
  getrusage(RUSAGE_SELF, &ru_begin);
  CLOCK_START(time_cond);
  cond(cond_metadata, 0);  //Compute the conductance of partition 0
  CLOCK_STOP(time_cond);
  struct rusage ru_end;
  getrusage(RUSAGE_SELF, &ru_end);
#ifdef PREFETCHER
  terminate_prefetch_thread();
  destroy_prefetcher();
#endif
  balloon_deflate();
  munmap(cond_metadata, graph->vertex_cnt*sizeof(cond_metadata_t));
  close_graph(graph);
  CLOCK_STOP(time_total);
  printf("TIME CONDUCTANCE %lu\n", time_cond);
  printf("TIME TOTAL %lu\n", time_total);
  print_rusage_stats(stdout, &ru_begin, &ru_end);
  return 0;
}
