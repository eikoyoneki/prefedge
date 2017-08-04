#include "graph_defs.h"
#include <math.h>


/* Assume _accum and _sample are long doubles, _cnt is integral */
#define UPDATE_WITH_SAMPLE(_accum, _cnt, _sample) do {		\
    (_accum) = (_accum)*((long double)((_cnt) - 1)/(_cnt));	\
    (_accum) += (_sample)/((long double)(_cnt));		\
  } while(0)


/* This structure tracks information for each vertex id */
typedef struct vertex_counter_st {
  unsigned long delta_hits;
  unsigned long target_hits;
} vertex_counter_t;

int main(int argc, char **argv)
{
  int fd_measure;
  unsigned long alist_entries_seen = 0, i;
  vertex_counter_t *counters;
  long double avg_delta_vertices = 0.0;
  long double avg_sq_delta_vertices = 0.0;
  long double avg_delta_edges = 0.0;
  long double avg_sq_delta_edges = 0.0;
  unsigned long edges_across_vertex_map_pages = 0;
  unsigned long edges_across_calist_map_pages = 0;
  unsigned long sample;
  unsigned char buffer[1024];
  unsigned long keep_compiler_happy;
  unsigned long clock_total, clock_scan1, clock_scan2;
  CLOCK_START(clock_total);
  if(argc <  3) {
    fprintf(stderr, "Usage %s graph_name output_file\n", argv[0]);
    exit(-1);
  } 
  graph_t *graph = open_graph(argv[1]);
  counters = (vertex_counter_t *)
    map_anon_memory(graph->vertex_cnt*sizeof(vertex_counter_t), "counters");
  CLOCK_START(clock_scan1);
  for(i=0;i<graph->vertex_cnt;i++) {
    edge_iterator_t iter;
    init_edge_iterator(graph, i, &iter);
    while(iter_step(graph, &iter) == 0) {
      counters[ABS_DIFF(i, iter.neighbour)].delta_hits++;
      counters[iter.neighbour].target_hits++;
      alist_entries_seen++;
      /* Compute reference deltas */
      unsigned long my_offset = BYTE_INDEX(graph->vertex_map[i].offset_edges);
      unsigned long other_offset =
	BYTE_INDEX(graph->vertex_map[iter.neighbour].offset_edges);
      sample = ABS_DIFF(my_offset, other_offset); 
      UPDATE_WITH_SAMPLE(avg_delta_edges, alist_entries_seen, (double)sample);
      UPDATE_WITH_SAMPLE(avg_sq_delta_edges, alist_entries_seen,
			 (long double)(sample*sample));
      sample = ABS_DIFF(i, iter.neighbour)*sizeof(vertex_t);
      UPDATE_WITH_SAMPLE(avg_delta_vertices, alist_entries_seen,
			 (long double)sample);
      UPDATE_WITH_SAMPLE(avg_sq_delta_vertices, alist_entries_seen,
			 (long double)(sample*sample));
      if((ABS_DIFF(i, iter.neighbour)*sizeof(vertex_t)) >= ASSUME_PAGE_SIZE) {
	edges_across_vertex_map_pages++;
      }
      if(ABS_DIFF(my_offset, other_offset) >= ASSUME_PAGE_SIZE) {
	edges_across_calist_map_pages++;
      }
    }
  }
  CLOCK_STOP(clock_scan1);
  /* Sanity check */
  assert(graph->alist_entries == alist_entries_seen);
  /* Rollup */
  fd_measure = open(argv[2], O_RDWR|O_CREAT|O_TRUNC, S_IRWXU);
  if(fd_measure == -1) {
    perror("Unable to open measures file:");
    exit(-1);
  }
  long double entropy_delta = 1.0, entropy_direct = 0.0, prob;
  CLOCK_START(clock_scan2);
  for(i=0;i<graph->vertex_cnt;i++) {
    if(counters[i].target_hits > 0) {
      prob = ((long double)counters[i].target_hits)/alist_entries_seen;
      if(prob > 0.0) { /* is this check necessary ? */
	entropy_direct += prob*log2l(1.0/prob);
      }
    }
    if(counters[i].delta_hits > 0) {
      prob = ((long double)counters[i].delta_hits)/alist_entries_seen;
      if(prob > 0.0) { /* is this check necessary ? */
	entropy_delta += prob*log2l(1.0/prob);
      }
    }
  }
  CLOCK_STOP(clock_scan2);
  snprintf(buffer, 1024, "ENTROPY_DIRECT %.3Lf\n", entropy_direct);
  keep_compiler_happy = write(fd_measure, buffer, strlen(buffer));
  assert(keep_compiler_happy == strlen(buffer));
  snprintf(buffer, 1024, "ENTROPY_DELTA %.3Lf\n", entropy_delta);
  keep_compiler_happy = write(fd_measure, buffer, strlen(buffer));
  assert(keep_compiler_happy == strlen(buffer));
  snprintf(buffer, 1024, "DELTA_VERTICES_MEAN %.3Lf\n", avg_delta_vertices);
  keep_compiler_happy = write(fd_measure, buffer, strlen(buffer));
  assert(keep_compiler_happy == strlen(buffer));
  snprintf(buffer, 1024, "DELTA_VERTICES_SD %.3Lf\n", 
	   (long double)sqrt(avg_sq_delta_vertices -
			     (avg_delta_vertices*avg_delta_vertices)));
  keep_compiler_happy = write(fd_measure, buffer, strlen(buffer));
  assert(keep_compiler_happy == strlen(buffer));
  snprintf(buffer, 1024, "DELTA_EDGES_MEAN %.3Lf\n", avg_delta_edges);
  keep_compiler_happy = write(fd_measure, buffer, strlen(buffer));
  assert(keep_compiler_happy == strlen(buffer));
  snprintf(buffer, 1024, "DELTA_EDGES_SD %.3Lf\n", 
	   (long double)sqrt(avg_sq_delta_edges -
		(avg_delta_edges*avg_delta_edges)));
  keep_compiler_happy = write(fd_measure, buffer, strlen(buffer));
  assert(keep_compiler_happy == strlen(buffer));
  snprintf(buffer, 1024, "PROB_VERTEX_PAGE_CROSSING %.3Lf\n", 
	   ((long double)edges_across_vertex_map_pages)/graph->alist_entries);
  keep_compiler_happy = write(fd_measure, buffer, strlen(buffer));
  assert(keep_compiler_happy == strlen(buffer));
  snprintf(buffer, 1024, "PROB_CALIST_PAGE_CROSSING %.3Lf\n", 
	   ((long double)edges_across_calist_map_pages)/graph->alist_entries);
  keep_compiler_happy = write(fd_measure, buffer, strlen(buffer));
  assert(keep_compiler_happy == strlen(buffer));
  close(fd_measure);
  close_graph(graph);
  munmap(counters, graph->vertex_cnt*sizeof(vertex_counter_t));
  CLOCK_STOP(clock_total);
  printf("TIME SCAN1 %lu\n", clock_scan1);
  printf("TIME SCAN2 %lu\n", clock_scan2);
  printf("TIME TOTAL %lu\n", clock_total);
  return 0;
}
