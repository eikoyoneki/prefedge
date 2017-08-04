#include "graph_defs.h"
#include <limits.h>

unsigned long bfs(csr_t *graph, unsigned long start_node);

typedef struct bfs_metadata_st {
  int touched;
} bfs_metadata_t;


bfs_metadata_t *metadata;
static csr_t * volatile graph;

int main(int argc, char **argv) {
  unsigned long time_bfs, time_total, components;
  CLOCK_START(time_total);
  if (argc < 3) {
    fprintf(stderr, "Usage %s graph_name root_id\n", argv[0]);
    exit(-1);
  }
  graph = open_csr(argv[1]);
  //metadata = (bfs_metadata_t*) map_anon_memory(
  //    graph->vertex_cnt * sizeof(bfs_metadata_t), "vertex metadata");
  print_mlocked_memory();
  unsigned long root_id = atol(argv[2]);
  assert(root_id < graph->vertex_cnt);
  /* Perhaps mmap /dev/null instead ? */
  //memset(metadata, 0, graph->vertex_cnt * sizeof(bfs_metadata_t));
  struct rusage ru_begin;
  getrusage(RUSAGE_SELF, &ru_begin);
  CLOCK_START(time_bfs);
  components = bfs(graph, root_id);
  CLOCK_STOP(time_bfs);
  struct rusage ru_end;
  getrusage(RUSAGE_SELF, &ru_end);
  //munmap(metadata, graph->vertex_cnt * sizeof(bfs_metadata_t));
  close_csr(graph);
  CLOCK_STOP(time_total);
  printf("TIME BFS %lu\n", time_bfs);
  printf("TIME TOTAL %lu\n", time_total);
  print_rusage_stats(stdout, &ru_begin, &ru_end);
  return 0;
}
