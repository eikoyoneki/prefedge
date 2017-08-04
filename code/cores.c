#include "graph_defs.h"
#include "prefetcher.h"
typedef struct cores_metadata_st {
  volatile unsigned long deg;
  volatile unsigned long vert;
  volatile unsigned long pos;
} cores_metadata_t;

static volatile unsigned char part = 0;
static volatile unsigned long count = 0;
static cores_metadata_t *metadata;
static csr_t * volatile graph;


void prefetcher_random_callback(unsigned long *laf,
				unsigned long laf_size,
				unsigned long ift)
{
  unsigned long entries = 0;
  unsigned long local_count = count;
  while(entries != ift && local_count != graph->vertex_cnt) {
    unsigned long page = graph->index[metadata[local_count].vert];
    unsigned long end = graph->index[metadata[local_count].vert+1];
    page = page >> (ASSUME_PAGE_SHIFT + 3); /* offset is in bits ! */
    end = end >> (ASSUME_PAGE_SHIFT + 3);
    laf[entries] = page;
    if (end > page) laf[entries+(2*laf_size)] = end - page;
    entries++;
    local_count++;
  }
}


unsigned long prefetcher_sequential_callback(unsigned long* aux_offset)
{
  unsigned long offset = graph->index[metadata[count].vert];
  return offset >> (ASSUME_PAGE_SHIFT + 3);
}



static void cores(csr_t *graph)
{
  unsigned long n = graph->vertex_cnt;
  unsigned long d,md,i,start,num;
  unsigned long v,u,w,du,pu,pw;
  unsigned long* bin;
  csr_edge_iterator_t iter;
  md = 0;
  for(v=0; v<n; v++){
    if(v<n-1){
      d = (graph->index[v+1]-graph->index[v])/graph->bits_per_edge;
    }else{
      d = (graph->alist_entries*graph->bits_per_edge - graph->index[n-1])/graph->bits_per_edge;
    }
    metadata[v].deg = d;
    if(d>md){
        md = d;
    }
  }

  bin = (unsigned long*) map_anon_memory(md*sizeof(unsigned long), "core bins");
  memset(bin, 0, md*sizeof(unsigned long));
  for(v=0; v<n; v++){
    bin[metadata[v].deg]++;
  }
  start = 0;
  for(d=0; d<=md; d++){
    num = bin[d];
    bin[d] = start;
    start += num;
  }
  for(v=0; v<n; v++){
    metadata[v].pos = bin[metadata[v].deg];
    metadata[metadata[v].pos].vert = v;
    bin[metadata[v].deg]++;
  }
  for(d=md; d>0; d--){
    bin[d] = bin[d-1];
  }
  bin[0] = 0;
  for(i = 0; i<n; i++){
    count = i;
    v = metadata[i].vert;
    csr_init_edge_iterator(graph, v, &iter);
    while(csr_iter_step(graph, &iter) == 0) {
      u = iter.neighbour;
      if(metadata[u].deg > metadata[v].deg){
        du = metadata[u].deg;
        pu = metadata[u].pos;
        pw = bin[du];
        w = metadata[pw].vert;
        if(u!=w){
          metadata[u].pos = pw;
          metadata[v].pos = pu;
          metadata[pu].vert = w;
          metadata[pw].vert = u;
        }
        bin[du]++;
        metadata[u].deg--;
      }
    }
  }
}

int main(int argc, char **argv)
{
  unsigned long time_cores, time_total;
  CLOCK_START(time_total);
  if(argc < 2) {
    fprintf(stderr, "Usage %s graph_name\n", argv[0]);
    exit(-1);
  }
#ifdef PREFETCHER

  bind_master();
  init_prefetcher(prefetcher_random_callback,
		  NULL);
//		  prefetcher_sequential_callback);
#endif
  graph = open_csr(argv[1]);
  metadata = (cores_metadata_t*)
    map_anon_memory(graph->vertex_cnt*sizeof(cores_metadata_t), "vertex metadata");
  /* Perhaps mmap /dev/null instead ? */
  memset(metadata, 0, graph->vertex_cnt*sizeof(cores_metadata_t));
  print_mlocked_memory();
#ifdef PREFETCHER
  launch_prefetch_thread(graph->fd_calist);
#endif
  struct rusage ru_begin;
  getrusage(RUSAGE_SELF, &ru_begin);
  CLOCK_START(time_cores);
  cores(graph);
  CLOCK_STOP(time_cores);
  struct rusage ru_end;
  getrusage(RUSAGE_SELF, &ru_end);
#ifdef PREFETCHER
  terminate_prefetch_thread();
  destroy_prefetcher();
#endif
  munmap(metadata, graph->vertex_cnt*sizeof(cores_metadata_t));
  close_csr(graph);
  CLOCK_STOP(time_total);
  printf("TIME CORES %lu\n", time_cores);
  printf("TIME TOTAL %lu\n", time_total);
  print_rusage_stats(stdout, &ru_begin, &ru_end);
  return 0;
}
