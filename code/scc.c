#include "graph_defs.h"
#include "balloon.h"
#include "prefetcher.h"

typedef struct scc_metadata_st {
  unsigned int index;
  unsigned int lowlink;
  unsigned int component;
  unsigned long stack_next;         //DFS stack
  unsigned long cc_stack_next;     //SCC stack
  edge_iterator_t iter;
} scc_metadata_t;


unsigned long scc(graph_t *graph,
	 scc_metadata_t *metadata,
	 unsigned long start_node)
{
  unsigned long dfs_stack_top, scc_stack_top, i;
  unsigned long alist_entries_seen = 0, current_vertex;
  unsigned int component_count, index;
  dfs_stack_top = ULONG_MAX;
  scc_stack_top = ULONG_MAX;
  component_count = 0;
  index = 1;
  i = start_node;
  do {
    if(metadata[i].index == 0) {
      metadata[i].index = index;
      metadata[i].lowlink = index;
      index++;
      init_edge_iterator(graph, i, &metadata[i].iter);
      DFS_PUSH(dfs_stack_top, i, metadata);
      CC_PUSH(scc_stack_top, i, metadata);
      while(dfs_stack_top != ULONG_MAX) {
        current_vertex = dfs_stack_top;
        if(iter_step(graph, &metadata[current_vertex].iter) == 0) {
          if(!metadata[current_vertex].iter.incoming) {
            unsigned long target = metadata[current_vertex].iter.neighbour;
            if(metadata[target].index == 0) {
              metadata[target].index = index;
              metadata[target].lowlink = index;
              index++;
              init_edge_iterator(graph, target, &metadata[target].iter);
              DFS_PUSH(dfs_stack_top, target, metadata);
              CC_PUSH(scc_stack_top, target, metadata);
            }
            else if(metadata[target].cc_stack_next != 0){  //Target is in the SCC stack
              if(metadata[target].index < metadata[current_vertex].lowlink){
                metadata[current_vertex].lowlink = metadata[target].index;
              }
            }
          }
          alist_entries_seen++;
        }
        else {
          if(metadata[current_vertex].lowlink == metadata[current_vertex].index){
            component_count ++;
            unsigned long w;
            do{
              w = scc_stack_top;
              metadata[scc_stack_top].component = component_count;
              CC_POP(scc_stack_top,metadata);
            }while(w != current_vertex);
          }
          DFS_POP(dfs_stack_top, metadata);
          if(dfs_stack_top != ULONG_MAX &&
             metadata[current_vertex].lowlink < metadata[dfs_stack_top].lowlink){
            metadata[dfs_stack_top].lowlink = metadata[current_vertex].lowlink;
          }
        }
      }
    }
    i = i + 1;
    if(i >= graph->vertex_cnt) {
      i = 0;
    }
  } while(i != start_node);
  assert(alist_entries_seen == graph->alist_entries);
  return component_count;
}

int main(int argc, char **argv)
{
  unsigned long time_total, time_scc;
  unsigned long sc_components;
  CLOCK_START(time_total);
  if(argc < 3) {
    fprintf(stderr, "Usage %s graph_name root_id\n", argv[0]);
    exit(-1);
  }
  graph_t *graph = open_vertices(argv[1]);
  unsigned long root_id = atol(argv[2]);
  assert(root_id < graph->vertex_cnt);
  scc_metadata_t * scc_metadata = (scc_metadata_t *)
    map_anon_memory(graph->vertex_cnt*sizeof(scc_metadata_t), "vertex metadata");
  /* Perhaps mmap /dev/null instead ? */
  memset(scc_metadata, 0, graph->vertex_cnt*sizeof(scc_metadata_t));
  balloon_init();
  balloon_inflate(); /* Simulate semi-em conditions */
  open_cal(graph);
  struct rusage ru_begin;
  getrusage(RUSAGE_SELF, &ru_begin);
  CLOCK_START(time_scc);
  sc_components = scc(graph, scc_metadata, root_id);
  CLOCK_STOP(time_scc);
  struct rusage ru_end;
  getrusage(RUSAGE_SELF, &ru_end);
  balloon_deflate();
  munmap(scc_metadata, graph->vertex_cnt*sizeof(scc_metadata_t));
  close_graph(graph);
  CLOCK_STOP(time_total);
  printf("COMPONENTS %lu\n", sc_components);
  printf("TIME SCC %lu\n", time_scc);
  printf("TIME TOTAL %lu\n", time_total);
  print_rusage_stats(stdout, &ru_begin, &ru_end);
  return 0;
}
