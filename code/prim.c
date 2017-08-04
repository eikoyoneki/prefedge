#include "graph_defs.h"
#include "balloon.h"
#include "prefetcher.h"

typedef struct prim_metadata_st {
  char touched; /* 0 == unseen, 1 == seen, 2 == done */
  unsigned long heap_index;
  unsigned long root;
} prim_metadata_t;

#define HEAP_OFFSET (unsigned long)&(((prim_metadata_t *)0)->heap_index)
#define PRIM_CONTAINER_OF(_indexp) (prim_metadata_t *)(((unsigned long)(_indexp)) - HEAP_OFFSET)

static graph_t * volatile graph;
static volatile unsigned long vertex_position = 0;
static heap_t *heap;
static prim_metadata_t *metadata;
static unsigned long *rew_index;

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
      prim_metadata_t * current_metadata =
	PRIM_CONTAINER_OF(indexp);
      unsigned long current_vertex = current_metadata - &metadata[0];
      unsigned long page = graph->vertex_map[current_vertex].offset_edges;
      unsigned long aux_page = rew_index[current_vertex];
      aux_page = aux_page >> ASSUME_PAGE_SHIFT; 
      page = page >> (ASSUME_PAGE_SHIFT + 3); /* offset is in bits ! */
      if(laf[HASH_MODULO(page, laf_size)] != page) {
	laf[HASH_MODULO(page, laf_size)] = page;
	laf[HASH_MODULO(page, laf_size) + laf_size] = aux_page;
	entries++;
      }
    }
    i++;
  }
}

void prim(unsigned long start_node,
	  unsigned char *rew)
{
  unsigned long i;
  unsigned long alist_entries_seen = 0;
  unsigned long vertices_covered = 0;
  i = start_node;
  do {
    if(metadata[i].touched == 0) {
      vertex_position = i;
      metadata[i].touched = 1;
      heap_add(heap, 0.0, &metadata[i].heap_index);
      while(!heap_is_empty(heap)) {
        prim_metadata_t * current_metadata =
          PRIM_CONTAINER_OF(heap_get_min_index(heap));
        unsigned long current_vertex = current_metadata - &metadata[0];
        edge_iterator_t iter;
        init_edge_iterator(graph, current_vertex, &iter);
        unsigned long edge_count = 0;
        double current_vertex_cost = heap_get_min_key(heap);
        heap_remove_min(heap);
        vertices_covered++;
        current_metadata->touched = 2; /* off the heap */
        current_metadata->root = i;
        double * rew_costs = (double *)&rew[rew_index[current_vertex]];
        while(iter_step(graph, &iter) == 0) {
	  double edge_cost = rew_costs[edge_count++];
          if(!iter.incoming) {
            unsigned long target = iter.neighbour;
            if(metadata[target].touched == 0) {
              metadata[target].touched = 1;
              heap_add(heap, edge_cost, &metadata[target].heap_index);
            }
            else if(metadata[target].touched == 1) {
              if(heap_get_key(heap, metadata[target].heap_index) >(edge_cost)) {
                heap_reduce_key(heap, metadata[target].heap_index, (edge_cost));
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
  unsigned long clock_total, clock_prim;
  unsigned long keep_compiler_happy;
  CLOCK_START(clock_total);
  if(argc < 3) {
    fprintf(stderr, "Usage %s graph root_id", argv[0]);
    exit(-1);
  }
#ifdef PREFETCHER
  bind_master();
  init_prefetcher(prefetcher_random_callback, 
		  NULL) //prefetcher_sequential_callback);
#endif
  graph = open_vertices(argv[1]);
  heap = allocate_heap(graph->vertex_cnt);
  metadata = (prim_metadata_t *)
    map_anon_memory(graph->vertex_cnt*sizeof(prim_metadata_t), "prim metadata");
  memset(metadata, 0, graph->vertex_cnt*sizeof(prim_metadata_t));
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
  CLOCK_START(clock_prim);
  prim(root, rew);
  CLOCK_STOP(clock_prim);
  struct rusage ru_end;
  getrusage(RUSAGE_SELF, &ru_end);
#ifdef PREFETCHER
  terminate_prefetch_thread();
  destroy_prefetcher();
#endif
  balloon_deflate();
  munmap(metadata, graph->vertex_cnt*sizeof(prim_metadata_t));
  destroy_heap(heap);
  munmap(rew, rew_size);
  munmap(rew_index, graph->vertex_cnt*sizeof(unsigned long));
  close(fd_rew);
  close(fd_rew_index);
  close_graph(graph);
  CLOCK_STOP(clock_total);
  printf("TIME PRIM %lu\n", clock_prim);
  printf("TIME TOTAL %lu\n", clock_total);
  print_rusage_stats(stdout, &ru_begin, &ru_end);
  return 0;
}
