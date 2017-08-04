#include "graph_defs.h"
#include "balloon.h"
#include "prefetcher.h"
#include <math.h>


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

static graph_t * volatile graph;
static volatile unsigned long vertex_position = 0;
static heap_t *heap;
static sssp_metadata_t *metadata;
static unsigned long *rew_index;
static double *vertex_info;
static double tgt_lat, tgt_long;
static unsigned long found = 0;

unsigned long prefetcher_sequential_callback(unsigned long *auxp)
{
  unsigned long offset = graph->vertex_map[vertex_position].offset_edges;
  *auxp = (rew_index[vertex_position] >> ASSUME_PAGE_SHIFT);
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
      unsigned long page = graph->vertex_map[current_vertex].offset_edges;
      page = page >> (ASSUME_PAGE_SHIFT + 3); /* offset is in bits ! */
      unsigned long aux_page = rew_index[current_vertex];
      aux_page = aux_page >> ASSUME_PAGE_SHIFT; /* offset is in bits ! */
      if(laf[HASH_MODULO(page, laf_size)] != page) {
	laf[HASH_MODULO(page, laf_size)] = page;
	laf[HASH_MODULO(page, laf_size) + laf_size] = aux_page;
	entries++;
      }
    }
    i++;
  }
}

double heuristic(unsigned long vert)
{
  double src_lat, src_long;
  src_long = vertex_info[2*vert];
  src_lat = vertex_info[2*vert + 1];
  /* Haversine great circle formula */
  /* credit: http://www.movable-type.co.uk/scripts/latlong.html 
   * for the codified form 
   */
  double R = 6371; // km
  double dLat = ABS_DIFF(tgt_lat, src_lat);
  double dLon = ABS_DIFF(tgt_long, src_long);
  /* everything already in radians */
  double a = (sin(dLat/2) * sin(dLat/2)) +
    sin(dLon/2) * sin(dLon/2) * cos(src_lat) * cos(tgt_lat); 
  double c = 2 * atan2(sqrt(a), sqrt(1-a)); 
  double d = R * c;
  return d;
}

void astar(unsigned long start_node,
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
      while(!heap_is_empty(heap)) {
	sssp_metadata_t * current_metadata =
	  SSSP_CONTAINER_OF(heap_get_min_index(heap));
	unsigned long current_vertex = current_metadata - &metadata[0];
	edge_iterator_t iter;
	init_edge_iterator(graph, current_vertex, &iter);
	unsigned long edge_count = 0;
	double current_vertex_cost = heap_get_min_key(heap);
	heap_remove_min(heap);
	vertices_covered++;
	current_metadata->touched = 2; /* off the heap */
#ifndef NDEBUG
	current_metadata->reach_cost = current_vertex_cost;
	current_metadata->root = i;
	if(vertex_info[2*current_vertex] == tgt_long &&
	   vertex_info[2*current_vertex + 1] == tgt_lat) {
	  found = 1;
	}
#endif
	double * rew_costs = 
	  (double *)&rew[rew_index[current_vertex]];
	while(iter_step(graph, &iter) == 0) {
	  double edge_cost = rew_costs[edge_count++];
	  if(!iter.incoming) {
	    unsigned long target = iter.neighbour;
	    double actual_cost = current_vertex_cost + edge_cost +
	      heuristic(target);
	    if(metadata[target].touched == 0) {
	      metadata[target].touched = 1;
	      heap_add(heap, 
		       actual_cost, 
		       &metadata[target].heap_index);
	    }
	    else if(metadata[target].touched == 1) {
	      if(heap_get_key(heap, metadata[target].heap_index) > 
		 actual_cost) {
		heap_reduce_key(heap, 
				metadata[target].heap_index, 
				actual_cost);
	      } 
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
  } while(i != start_node);
  assert(alist_entries_seen == graph->alist_entries); 
  assert(vertices_covered == graph->vertex_cnt);
}

int main(int argc, char **argv)
{
  unsigned char string_buffer[1024];
  unsigned long clock_total, clock_sssp;
  unsigned long keep_compiler_happy;
  CLOCK_START(clock_total);
#ifdef TGT_VERTEX
  if(argc < 4) {
    fprintf(stderr, "Usage %s graph root_id tgt_id", argv[0]);
    exit(-1);
  }
  unsigned long tgt_vertex;
#else
  if(argc < 5) {
    fprintf(stderr, "Usage %s graph root_id tgt_lat(rad) tgt_long(rad)", argv[0]);
    exit(-1);
  }
#endif

#ifdef PREFETCHER
  bind_master();
  init_prefetcher(prefetcher_random_callback, 
		  prefetcher_sequential_callback);
#endif
#ifndef TGT_VERTEX
  tgt_lat  = atof(argv[3]);
  tgt_long = atof(argv[4]);
#else
  tgt_vertex = atol(argv[3]);
#endif
  graph = open_vertices(argv[1]);
  heap = allocate_heap(graph->vertex_cnt);
  metadata = (sssp_metadata_t *)
    map_anon_memory(graph->vertex_cnt*sizeof(sssp_metadata_t), "sssp metadata");
  memset(metadata, 0, graph->vertex_cnt*sizeof(sssp_metadata_t));
  strcpy(string_buffer, argv[1]);
  strcat(string_buffer, ".rew_index");
  int fd_rew_index = open(string_buffer, O_RDONLY|O_LARGEFILE, S_IRWXU);
  if(fd_rew_index == -1) {
    perror("Unable to open rew index file:");
    exit(-1);
  }
  rew_index = (unsigned long *)
    mmap(0,  (graph->vertex_cnt)*sizeof(unsigned long),
	 PROT_READ, MAP_SHARED|MAP_FILE, fd_rew_index, 0);
  if(rew_index == MAP_FAILED) {
    perror("Unable to map rew index:");
    exit(-1);
  }
  if(mlock(rew_index, (graph->vertex_cnt)*sizeof(unsigned long)) < 0) {
    perror("mlock failed on rew index:");
    exit(-1);
  }
  register_mlocked_memory(graph->vertex_cnt*sizeof(unsigned long));
  strcpy(string_buffer, argv[1]);
  strcat(string_buffer, ".co");
  int fd_vertex_info = open(string_buffer, O_RDONLY|O_LARGEFILE, S_IRWXU);
  if(fd_vertex_info == -1) {
    perror("Unable to open vertex info file:");
    exit(-1);
  }
  vertex_info = (double *)
    mmap(0,  2*(graph->vertex_cnt)*sizeof(double),
	 PROT_READ, MAP_SHARED|MAP_FILE, fd_vertex_info, 0);
  if(vertex_info == MAP_FAILED) {
    perror("Unable to map vertex info:");
    exit(-1);
  }
  if(mlock(vertex_info, 2*(graph->vertex_cnt)*sizeof(double)) < 0) {
    perror("mlock failed on vertex info:");
    exit(-1);
  }
  register_mlocked_memory(2*graph->vertex_cnt*sizeof(double));
#ifdef TGT_VERTEX
  tgt_long  = vertex_info[2*tgt_vertex];
  tgt_lat = vertex_info[2*tgt_vertex + 1];
#endif
  balloon_init();
  balloon_inflate(); /* Simulate semi-em conditions */
  open_cal(graph);
  unsigned long root = atol(argv[2]);
  assert(root < graph->vertex_cnt);
  strcpy(string_buffer, argv[1]);
  strcat(string_buffer, ".rew");
  int fd_rew = open(string_buffer, O_RDONLY|O_LARGEFILE, S_IRWXU);
  if(fd_rew == -1) {
    perror("Unable to open rew file:");
    exit(-1);
  }
  unsigned long rew_size = lseek(fd_rew, 0, SEEK_END);
  keep_compiler_happy = lseek(fd_rew, 0, SEEK_SET);
  assert(keep_compiler_happy == 0);
  unsigned char *rew = (unsigned char *)
    mmap(0,  rew_size,
	 PROT_READ, MAP_SHARED|MAP_FILE, fd_rew, 0);
  if(rew == MAP_FAILED) {
    perror("Unable to map rew:");
    exit(-1);
  }
#ifdef PREFETCHER
#ifdef AUX_PREFETCH
  prefetcher_set_aux_fd(fd_rew);
#endif
  launch_prefetch_thread(graph->fd_calist);
#endif
  struct rusage ru_begin;
  getrusage(RUSAGE_SELF, &ru_begin);
  CLOCK_START(clock_sssp);
  astar(root, rew);
  CLOCK_STOP(clock_sssp);
  struct rusage ru_end;
  getrusage(RUSAGE_SELF, &ru_end);
#ifdef PREFETCHER
  terminate_prefetch_thread();
  destroy_prefetcher();
#endif
  balloon_deflate();
  munmap(metadata, graph->vertex_cnt*sizeof(sssp_metadata_t));
  destroy_heap(heap);
  munmap(rew, rew_size);
  munmap(rew_index, graph->vertex_cnt*sizeof(unsigned long));
  munmap(vertex_info, 2*graph->vertex_cnt*sizeof(double));
  close(fd_rew);
  close(fd_rew_index);
  close(fd_vertex_info);
  close_graph(graph);
  CLOCK_STOP(clock_total);
  printf("TIME ASTAR %lu\n", clock_sssp);
  printf("TIME TOTAL %lu\n", clock_total);
  printf("FOUND %lu\n", found);
  print_rusage_stats(stdout, &ru_begin, &ru_end);
  return 0;
}
