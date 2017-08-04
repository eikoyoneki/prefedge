#include "graph_defs.h"
#include "prefetcher.h"
#include "balloon.h"


typedef struct mis_metadata_st {
  unsigned char inmis;  //0: not decided, 1: no, 2: yes
} mis_metadata_t;

static volatile unsigned long vertex_position = 0;
static graph_t * volatile graph;

unsigned long prefetcher_sequential_callback(unsigned long *auxp)
{
  unsigned long offset = graph->vertex_map[vertex_position].offset_edges;
  return offset >> (ASSUME_PAGE_SHIFT + 3);
}

void mis(mis_metadata_t *metadata,
	 unsigned long part)
{
  unsigned long i;
  for(i=0; i<graph->vertex_cnt; i++) {
    vertex_position = i;
    if(metadata[i].inmis == 0){
      metadata[i].inmis = 2;
      edge_iterator_t iter;
      init_edge_iterator(graph, i, &iter);
      while(iter_step(graph, &iter) == 0) {
        if(iter.neighbour > i){
          metadata[iter.neighbour].inmis = 1;
        }
      }
    }
  }
}

int main(int argc, char **argv)
{
  unsigned long time_mis, time_total;
  CLOCK_START(time_total);
  if(argc < 2) {
    fprintf(stderr, "Usage %s graph_name\n", argv[0]);
    exit(-1);
  }
#ifdef PREFETCHER
  bind_master();
  init_prefetcher(NULL, prefetcher_sequential_callback);
#endif
  graph = open_vertices(argv[1]);
  mis_metadata_t * mis_metadata = (mis_metadata_t*)
    map_anon_memory(graph->vertex_cnt*sizeof(mis_metadata_t), "vertex metadata");
  memset(mis_metadata, 0, graph->vertex_cnt*sizeof(mis_metadata_t));
  balloon_init();
  balloon_inflate(); /* Simulate semi-em conditions */
  open_cal(graph);
#ifdef PREFETCHER
  launch_prefetch_thread(graph->fd_calist);
#endif
  struct rusage ru_begin;
  getrusage(RUSAGE_SELF, &ru_begin);
  CLOCK_START(time_mis);
  mis(mis_metadata, 0);
  CLOCK_STOP(time_mis);
  struct rusage ru_end;
  getrusage(RUSAGE_SELF, &ru_end);
#ifdef PREFETCHER
  terminate_prefetch_thread();
  destroy_prefetcher();
#endif
  balloon_deflate();
  munmap(mis_metadata, graph->vertex_cnt*sizeof(mis_metadata_t));
  close_graph(graph);
  CLOCK_STOP(time_total);
  printf("TIME MIS %lu\n", time_mis);
  printf("TIME TOTAL %lu\n", time_total);
  print_rusage_stats(stdout, &ru_begin, &ru_end);
  return 0;
}
