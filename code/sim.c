#include "graph_defs.h"
#include "balloon.h"
#include "prefetcher.h"


typedef struct sim_metadata_st {
  double p1_old;
  double p1_new;
  double p2_old;
  double p2_new;
  unsigned long degree;
  volatile unsigned long list1_next;
  volatile unsigned long list2_next;
  char inl1;
  char inl2;
} sim_metadata_t;

static volatile unsigned long queue_head = ULONG_MAX;
static sim_metadata_t *metadata;
static volatile unsigned long current_active_list = 0;
static graph_t * volatile graph;

void prefetcher_random_callback(unsigned long *laf,
				unsigned long laf_size,
				unsigned long ift)
{
  unsigned long current_hoq;
  unsigned long current_list;
  unsigned long entries = 0;
  /* Fill in inner-loop entries from BFS queue */
  current_hoq = queue_head;
  current_list = current_active_list;
  if(current_hoq != ULONG_MAX) {
    if(current_list == 1) {
      current_hoq = metadata[current_hoq].list1_next;
    }
    else if(current_list == 2) {
      current_hoq = metadata[current_hoq].list2_next;
    }
    else {
      assert(0);
    }
  }
  while(entries != ift && current_hoq != ULONG_MAX) {
    unsigned long page = graph->vertex_map[current_hoq].offset_edges;
    page = page >> (ASSUME_PAGE_SHIFT + 3); /* offset is in bits ! */
    if(laf[HASH_MODULO(page, laf_size)] != page) {
      laf[HASH_MODULO(page, laf_size)] = page;
      entries++;
    }
    if(current_list == 1) {
      current_hoq = metadata[current_hoq].list1_next;
    }
    else if(current_list == 2) {
      current_hoq = metadata[current_hoq].list2_next;
    }
    else {
      assert(0);
    }
  }
}


void sim(unsigned long x,
	 unsigned long y,
	 unsigned long time,
	 double * lrw,
	 double * srw)
{
  unsigned long t, list1_last, list2_last;
  *lrw = 0.0;
  *srw = 0.0;
  metadata[x].p1_new = 1.0;
  metadata[y].p2_new = 1.0;

  //Initialise the lists and metadata of x and y
  list1_last = x;
  metadata[x].list1_next = ULONG_MAX;
  metadata[x].inl1 = 1;
  edge_iterator_t itx, ity;
  init_edge_iterator(graph, x, &itx);
  metadata[x].degree = 0;
  while(iter_step(graph,&itx) == 0) {
    metadata[x].degree++;
  }
  list2_last = y;
  metadata[y].list2_next = ULONG_MAX;
  metadata[y].inl2 = 1;
  init_edge_iterator(graph, y, &ity);
  metadata[y].degree = 0;
  while(iter_step(graph,&ity) == 0) {
    metadata[y].degree++;
  }

  for(t=0; t< time; t++){
    unsigned long iter;
    //First we store the old proportions
    iter = list1_last;
    int count = 0;
    while(iter != ULONG_MAX){
      metadata[iter].p1_old = metadata[iter].p1_new;
      metadata[iter].p1_new = 0.0;
      LIST1_NEXT(iter,metadata);
    }
    iter = list2_last;
    while(iter != ULONG_MAX){
      metadata[iter].p2_old = metadata[iter].p2_new;
      metadata[iter].p2_new = 0.0;
      LIST2_NEXT(iter,metadata);
    }
    //Then we push the messages into the new proportions
    //Note that we iterate through the list from the last element,
    //that way we do not iterate through elements that were added
    //on the same iteration

    iter = list1_last;
    current_active_list = 1;
    queue_head = list1_last;
    while(iter != ULONG_MAX){
      edge_iterator_t edge_iter;
      init_edge_iterator(graph, iter, &edge_iter);
      double mess = metadata[iter].p1_old / (double) metadata[iter].degree;
      while(iter_step(graph, &edge_iter) == 0) {
        if(metadata[edge_iter.neighbour].inl1 == 0){
          LIST1_PUSH(list1_last, edge_iter.neighbour, metadata);
          if(metadata[edge_iter.neighbour].degree == 0){
            edge_iterator_t edge_iter2;
            init_edge_iterator(graph, edge_iter.neighbour, &edge_iter2);
            while(iter_step(graph,&edge_iter2) == 0) {
              metadata[edge_iter.neighbour].degree++;
            }
          }
          metadata[edge_iter.neighbour].inl1 = 1;
        }
        metadata[edge_iter.neighbour].p1_new += mess;
      }
      LIST1_NEXT(iter,metadata);
      queue_head = iter;
    }


    iter = list2_last;
    current_active_list = 2;
    queue_head = list2_last;
    while(iter != ULONG_MAX){
      edge_iterator_t edge_iter;
      init_edge_iterator(graph, iter, &edge_iter);
      double mess = metadata[iter].p2_old / metadata[iter].degree;
      while(iter_step(graph, &edge_iter) == 0) {
        if(metadata[edge_iter.neighbour].inl2 == 0){
          LIST2_PUSH(list2_last, edge_iter.neighbour, metadata);
          if(metadata[edge_iter.neighbour].degree == 0){
            edge_iterator_t edge_iter2;
            init_edge_iterator(graph, edge_iter.neighbour, &edge_iter2);
            while(iter_step(graph,&edge_iter2) == 0) {
              metadata[edge_iter.neighbour].degree++;
            }
          }
          metadata[edge_iter.neighbour].inl2 = 1;
        }
        metadata[edge_iter.neighbour].p2_new += mess;
      }
      LIST2_NEXT(iter,metadata);
      queue_head = iter;
    }

    *lrw = (metadata[x].degree * metadata[y].p1_new + metadata[y].degree * metadata[x].p2_new) / (graph->alist_entries);
    *srw += *lrw;
  }
}

int main(int argc, char **argv)
{
  unsigned long time_sim, time_total, node1, node2, time;
  CLOCK_START(time_total);
  if(argc < 5) {
    fprintf(stderr, "Usage %s graph_name node1 node2 number of steps\n", argv[0]);
    exit(-1);
  }
#ifdef PREFETCHER
  bind_master();
  init_prefetcher(prefetcher_random_callback, NULL);
#endif
  graph = open_vertices(argv[1]);
  node1 = atol(argv[2]);
  node2 = atol(argv[3]);
  time = atol(argv[4]);
  metadata = (sim_metadata_t*)
    map_anon_memory(graph->vertex_cnt*sizeof(sim_metadata_t), "vertex metadata");
  memset(metadata, 0, graph->vertex_cnt*sizeof(sim_metadata_t));
  balloon_init();
  balloon_inflate(); /* Simulate semi-em conditions */
  open_cal(graph);
  double lrw, srw;
#ifdef PREFETCHER
  launch_prefetch_thread(graph->fd_calist);
#endif
  struct rusage ru_begin;
  getrusage(RUSAGE_SELF, &ru_begin);
  CLOCK_START(time_sim);
  sim(node1, node2, time, &lrw, &srw);
  CLOCK_STOP(time_sim);
  struct rusage ru_end;
  getrusage(RUSAGE_SELF, &ru_end);
#ifdef PREFETCHER
  terminate_prefetch_thread();
  destroy_prefetcher();
#endif
  balloon_deflate();
  munmap(metadata, graph->vertex_cnt*sizeof(sim_metadata_t));
  close_graph(graph);
  CLOCK_STOP(time_total);
  printf("TIME SIMILARITY %lu\n", time_sim);
  printf("TIME TOTAL %lu\n", time_total);
  print_rusage_stats(stdout, &ru_begin, &ru_end);
  return 0;
}
