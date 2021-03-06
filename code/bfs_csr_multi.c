#include "graph_defs.h"
#include "balloon.h"
#include "prefetcher.h"

typedef struct bfs_metadata_st {
  char touched;
  volatile unsigned long queue_next;
} bfs_metadata_t;

static volatile unsigned long queue_head = ULONG_MAX;
static volatile unsigned long vertex_position = 0;
static bfs_metadata_t *metadata;
static csr_t * volatile graph;

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
    unsigned long page = graph->index[current_hoq];
    unsigned long end = graph->index[current_hoq+1];

    page = page >> (ASSUME_PAGE_SHIFT + 3); /* offset is in bits ! */
    end = end >> (ASSUME_PAGE_SHIFT + 3);
    
    for (; page <= end; page++) {
      if(laf[HASH_MODULO(page, laf_size)] != page) {
        laf[HASH_MODULO(page, laf_size)] = page;
        entries++;
        if (entries==ift) break;
      }
    } 
    current_hoq = metadata[current_hoq].queue_next;
  }
}


unsigned long prefetcher_sequential_callback(unsigned long* aux_offset)
{
  unsigned long offset = graph->index[vertex_position];
  return offset >> (ASSUME_PAGE_SHIFT + 3);
}

unsigned long total_queue_demands = 0;
unsigned long queue_above_threshold = 0;
unsigned long queue_length = 0;

/* returns number of connected components */
static unsigned long bfs(csr_t *graph, 
			 unsigned long start_node)
{
  unsigned long i;
  unsigned long alist_entries_seen = 0, current_vertex;
  unsigned long components = 0;
  unsigned long queue_tail = ULONG_MAX;
  unsigned long time_comp, time_giant = 0, id_giant;
  i = start_node;
  do {
    vertex_position = i;
    if(metadata[i].touched == 0) {
      CLOCK_START(time_comp);
      metadata[i].touched = 1;
      components++;
      BFS_PUSH(queue_head, queue_tail, i, metadata);
      queue_length = 1;
      while(queue_head != ULONG_MAX) {
	total_queue_demands++;
	if(queue_length >= RASP_THRESHOLD) {
	  queue_above_threshold++;
	}
	current_vertex = BFS_POP(queue_head, queue_tail, metadata);
	queue_length--;
	csr_edge_iterator_t iter;
	csr_init_edge_iterator(graph, current_vertex, &iter);
	while(csr_iter_step(graph, &iter) == 0) {
	  if(!iter.incoming) {
	    unsigned long target = iter.neighbour;
	    if(metadata[target].touched == 0) {
	      metadata[target].touched = 1;
	      BFS_PUSH(queue_head, queue_tail, target, metadata);
	      queue_length++;
	    }
	  }
	  alist_entries_seen++;
	}
      }
      CLOCK_STOP(time_comp);
      if (time_comp > time_giant) {
        time_giant = time_comp;
        id_giant = i;
      }
    }
    i = i + 1;
    if(i >= graph->vertex_cnt) {
      i = 0;
    }
  } while(i != start_node);
  assert(alist_entries_seen == graph->alist_entries); 
  printf("TIME GIANT COMP %lu\n", time_giant);
  printf("ID GIANT COMP %lu\n", id_giant);
  return components;
}

int main(int argc, char **argv)
{
  unsigned long time_bfs, time_total, components;
  CLOCK_START(time_total);
  if(argc < 3) {
    fprintf(stderr, "Usage %s graph_name root_id\n", argv[0]);
    exit(-1);
  }
#ifdef PREFETCHER
  bind_master();
  init_prefetcher(prefetcher_random_callback, 
			NULL);
//		  prefetcher_sequential_callback);
#endif
  graph = open_csr_vertices(argv[1]);
  metadata = (bfs_metadata_t*)
    map_anon_memory(graph->vertex_cnt*sizeof(bfs_metadata_t), "vertex metadata");
  balloon_init();
  balloon_inflate(); /* Simulate semi-em conditions */
  open_csr(graph);
  unsigned long root_id = atol(argv[2]);
  assert(root_id < graph->vertex_cnt);
  /* Perhaps mmap /dev/null instead ? */
  memset(metadata, 0, graph->vertex_cnt*sizeof(bfs_metadata_t));
#ifdef PREFETCHER
  launch_prefetch_thread(graph->fd_calist);
#endif
  struct rusage ru_begin;
  getrusage(RUSAGE_SELF, &ru_begin);
  CLOCK_START(time_bfs);
  components = bfs(graph, root_id);
  CLOCK_STOP(time_bfs);
  struct rusage ru_end;
  getrusage(RUSAGE_SELF, &ru_end);
#ifdef PREFETCHER
  terminate_prefetch_thread();
  destroy_prefetcher();
#endif
  balloon_deflate();
  munmap(metadata, graph->vertex_cnt*sizeof(bfs_metadata_t));
  close_csr(graph);
  CLOCK_STOP(time_total);
  printf("COMPONENTS %lu\n", components);
  printf("TIME BFS %lu\n", time_bfs);
  printf("TIME TOTAL %lu\n", time_total);
  print_rusage_stats(stdout, &ru_begin, &ru_end);
  printf("F_THRESHOLD %f\n", ((double)queue_above_threshold)/total_queue_demands);
  return 0;
}
