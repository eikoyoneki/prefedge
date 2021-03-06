#include "graph_defs.h"
#include "prefetcher.h"
#include <time.h>

typedef struct bfs_metadata_st {
  char touched;
  volatile unsigned long queue_next;
} bfs_metadata_t;

static volatile unsigned long queue_head = ULONG_MAX;
static volatile unsigned long vertex_position = 0;
static bfs_metadata_t *metadata;
static csr_t * volatile graph;
unsigned long MAX_CACHE = ULONG_MAX;
long MIN_CACHE = 0;
unsigned long visited = 0;


void prefetcher_random_callback(unsigned long *laf,
				unsigned long laf_size,
				unsigned long ift)
{
  static unsigned long old_hoq = ULONG_MAX;
  unsigned long current_hoq = ULONG_MAX;
  static unsigned long ra_depth = 0;
  static char preload = 0;
  static long pf_visited = 0;
  unsigned long entries = 0;
  /* Fill in inner-loop entries from BFS queue */
  /*
  if ((preload == 0) && (ra_depth > MAX_CACHE)) {
    preload = 1;
    current_hoq = ULONG_MAX;
  }
*/
//  fprintf(stderr, "BFS: %ld PF: %ld\n", visited, pf_visited); 

  current_hoq = old_hoq;
  if ((current_hoq == ULONG_MAX) || (((signed long)(pf_visited - visited)) > MIN_CACHE)/*|| (ra_depth > MIN_CACHE)*/) {
    current_hoq = queue_head;
    pf_visited = visited;
//    ra_depth = 0;
//    fprintf(stderr, "Resetting\n");
  }
//  if (((signed long)(pf_visited - visited)) > MIN_CACHE) return;
/*  if(current_hoq != ULONG_MAX) {
    current_hoq = metadata[current_hoq].queue_next;
  }
*/
  while(entries != ift && current_hoq != ULONG_MAX) {
    unsigned long page = graph->index[current_hoq];
    unsigned long end = graph->index[current_hoq+1];
    page = page >> (ASSUME_PAGE_SHIFT + 3); /* offset is in bits ! */
    end = end >> (ASSUME_PAGE_SHIFT + 3);
//    if(laf[HASH_MODULO(page, laf_size)] != page) {
//      laf[HASH_MODULO(page, laf_size)] = page;
//    for (; page <= end; page++) {
//	if (entries==ift) break;
	laf[entries] = page;
	if (end > page) laf[entries+(2*laf_size)] = end - page;
      entries++;
//    }
//    }
    old_hoq = current_hoq;
    current_hoq = metadata[current_hoq].queue_next;
    pf_visited++;
  }
  ra_depth += entries;
}


unsigned long prefetcher_sequential_callback(unsigned long* aux_offset)
{
  unsigned long offset = graph->index[vertex_position];
  return offset >> (ASSUME_PAGE_SHIFT + 3);
}

unsigned long total_queue_demands = 0;
unsigned long queue_above_threshold = 0;
unsigned long queue_length = 0;
unsigned long time_sample[1000];
unsigned int tcount = 0;
unsigned char edgeflag = 0;
struct timespec t1; 
struct timespec t2;
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
	visited++;
	queue_length--;
	csr_edge_iterator_t iter;
	csr_init_edge_iterator(graph, current_vertex, &iter);
	clock_gettime(CLOCK_REALTIME, &t1);
	edgeflag = 0;
	while(csr_iter_step(graph, &iter) == 0) {
	  clock_gettime(CLOCK_REALTIME, &t2);
	  if (t2.tv_nsec > t1.tv_nsec) 
		t2.tv_nsec -= t1.tv_nsec;
	  else{
		t2.tv_nsec =1000000000 - (t2.tv_nsec - t1.tv_nsec);
	   }
	   tcount++;
	   if ((edgeflag==0) && (tcount > 999) && (tcount < 2000)) 
	   	time_sample[tcount] = t2.tv_nsec;
	   if(!iter.incoming) {
	    unsigned long target = iter.neighbour;
	    if(metadata[target].touched == 0) {
	      metadata[target].touched = 1;
	      BFS_PUSH(queue_head, queue_tail, target, metadata);
	      queue_length++;
	    }
            edgeflag = 1;
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
 char *env_var;
  env_var = getenv("CMAX");
  if(env_var != NULL) {
    MAX_CACHE = atol(env_var);
  }
  env_var = getenv("CMIN");
  if(env_var != NULL) {
    MIN_CACHE = atol(env_var);
  }

  bind_master();
  init_prefetcher(prefetcher_random_callback,
		  NULL); 
//		  prefetcher_sequential_callback);
#endif
  graph = open_csr(argv[1]);
  metadata = (bfs_metadata_t*)
    map_anon_memory(graph->vertex_cnt*sizeof(bfs_metadata_t), "vertex metadata");
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
  munmap(metadata, graph->vertex_cnt*sizeof(bfs_metadata_t));
  close_csr(graph);
  CLOCK_STOP(time_total);
  printf("COMPONENTS %lu\n", components);
  printf("TIME BFS %lu\n", time_bfs);
  printf("TIME TOTAL %lu\n", time_total);
  print_rusage_stats(stdout, &ru_begin, &ru_end);
  printf("F_THRESHOLD %f\n", ((double)queue_above_threshold)/total_queue_demands);
  int i;
  for (i = 0; i < 1000; i++)
 	printf("%ld\n", time_sample[i]);

  return 0;
}
