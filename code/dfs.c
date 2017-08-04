#include "graph_defs.h"
#include "balloon.h"

typedef struct dfs_metadata_st {
  char touched;
  unsigned long stack_next;
  edge_iterator_t iter;
} dfs_metadata_t;


void dfs(graph_t *graph, 
	 dfs_metadata_t *metadata,
	 unsigned long start_node)
{
  unsigned long stack_top, i;
  unsigned long alist_entries_seen = 0, current_vertex;
  stack_top = ULONG_MAX;
  i = start_node;
  do {
    if(metadata[i].touched == 0) {
      metadata[i].touched = 1;
      init_edge_iterator(graph, i, &metadata[i].iter);
      DFS_PUSH(stack_top, i, metadata);
      while(stack_top != ULONG_MAX) {
	current_vertex = stack_top;
	if(iter_step(graph, &metadata[current_vertex].iter) == 0) {
	  if(!metadata[current_vertex].iter.incoming) {
	    unsigned long target = metadata[current_vertex].iter.neighbour;
	    if(metadata[target].touched == 0) {
	      metadata[target].touched = 1;
	      init_edge_iterator(graph, target, &metadata[target].iter);
	      DFS_PUSH(stack_top, target, metadata);
	    }
	  }
	  alist_entries_seen++;
	}
	else {
	  DFS_POP(stack_top, metadata);
	}
      }
    }
    i = i + 1;
    if(i >= graph->vertex_cnt) {
      i = 0;
    }
  } while(i != start_node);
  assert(alist_entries_seen == graph->alist_entries); 
}

int main(int argc, char **argv)
{
  unsigned long time_total, time_dfs;
  CLOCK_START(time_total);
  if(argc < 3) {
    fprintf(stderr, "Usage %s graph_name root_id\n", argv[0]);
    exit(-1);
  }
  graph_t *graph = open_vertices(argv[1]);
  unsigned long root_id = atol(argv[2]);
  assert(root_id < graph->vertex_cnt);
  dfs_metadata_t * dfs_metadata = (dfs_metadata_t *)
    map_anon_memory(graph->vertex_cnt*sizeof(dfs_metadata_t), "vertex metadata");
  /* Perhaps mmap /dev/null instead ? */
  memset(dfs_metadata, 0, graph->vertex_cnt*sizeof(dfs_metadata_t));
  balloon_init();
  balloon_inflate(); /* Simulate semi-em conditions */
  open_cal(graph);
  struct rusage ru_begin;
  getrusage(RUSAGE_SELF, &ru_begin);
  CLOCK_START(time_dfs);
  dfs(graph, dfs_metadata, root_id);
  CLOCK_STOP(time_dfs);
  struct rusage ru_end;
  getrusage(RUSAGE_SELF, &ru_end);
  balloon_deflate();
  munmap(dfs_metadata, graph->vertex_cnt*sizeof(dfs_metadata_t));
  close_graph(graph);
  CLOCK_STOP(time_total);
  printf("TIME DFS %lu\n", time_dfs);
  printf("TIME TOTAL %lu\n", time_total);
  print_rusage_stats(stdout, &ru_begin, &ru_end);
  return 0;
}
