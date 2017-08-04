#include "graph_defs.h"

int main(int argc, char **argv)
{
  unsigned char string_buffer[1024];
  int fd_rew, fd_rew_index;
  unsigned long i, j, keep_compiler_happy;
  unsigned long time_total;
  CLOCK_START(time_total);
  if(argc < 2) {
    fprintf(stderr, "Usage: %s graph\n", argv[0]);
    exit(-1);
  }
  csr_t *graph = open_csr(argv[1]);
  strcpy(string_buffer, argv[1]);
  strcat(string_buffer, ".rew");
  fd_rew = open(string_buffer, O_RDWR|O_CREAT|O_TRUNC|O_LARGEFILE, S_IRWXU);
  if(fd_rew == -1) {
    perror("Unable to open rew file:");
    exit(-1);
  }
  strcpy(string_buffer, argv[1]);
  strcat(string_buffer, ".rew_index");
  fd_rew_index = open(string_buffer, O_RDWR|O_CREAT|O_TRUNC|O_LARGEFILE, S_IRWXU);
  if(fd_rew_index == -1) {
    perror("Unable to open rew index file:");
    exit(-1);
  }
  unsigned long current_offset = 0;
  unsigned long alist_entries_seen = 0;
  srand48(0xdeadbeef); /* for repeatability */

#ifndef __APPLE__
  if(posix_fallocate(fd_rew_index, 0, graph->vertex_cnt*sizeof(unsigned long))) {
    perror("Unable to allocate space for rew index:");
    exit(-1);
  }
  if(posix_fallocate(fd_rew, 0, graph->alist_entries*sizeof(double))) {
    perror("Unable to allocate space for rew:");
    exit(-1);
  }
#else
  for(i=0;i<graph->vertex_cnt;i++) {
    unsigned long zero = 0;
    keep_compiler_happy = write(fd_rew_index, &zero, sizeof(unsigned long));
  }
  keep_compiler_happy = lseek(fd_rew_index, 0, SEEK_SET);
  for(i=0;i<graph->alist_entries;i++) {
    double zero = 0.0;
    keep_compiler_happy = write(fd_rew, &zero, sizeof(double));
  }
  keep_compiler_happy = lseek(fd_rew, 0, SEEK_SET);
#endif
  double *fd_rew_map;
  unsigned long * fd_rew_index_map;
  fd_rew_map = (double *)
    mmap(0, graph->alist_entries*sizeof(double), PROT_READ|PROT_WRITE,
	 MAP_FILE|MAP_SHARED, fd_rew, 0);
  if(fd_rew_map == MAP_FAILED) {
    perror("Unable to map in rew:");
    exit(-1);
  }
  fd_rew_index_map = (unsigned long *)
    mmap(0, graph->vertex_cnt*sizeof(unsigned long), PROT_READ|PROT_WRITE,
	 MAP_FILE|MAP_SHARED, fd_rew_index, 0);
  if(fd_rew_index_map == MAP_FAILED) {
    perror("Unable to map in rew index:");
    exit(-1);
  }
  fprintf(stderr, "Starting generation\n");
  fflush(stderr);
  unsigned long periodic_clock;
  unsigned long target_edges = 1000000;
  CLOCK_START(periodic_clock);
  for(i=0;i<graph->vertex_cnt;i++) {
    fd_rew_index_map[i] = current_offset;
    csr_edge_iterator_t iter;
    csr_init_edge_iterator(graph, i, &iter);
    while(csr_iter_step(graph, &iter) == 0) {
      double eweight = drand48() * (graph->vertex_cnt);
      fd_rew_map[alist_entries_seen++] = eweight;
      current_offset += sizeof(double);
      target_edges--;
      if(target_edges == 0) {
	CLOCK_STOP(periodic_clock);
	fprintf(stderr, "Generated weights for 1000000 edges time %lu\n", periodic_clock);
	CLOCK_START(periodic_clock);
	target_edges = 1000000;
      }
    }
  }
  /* Sanity check */
  assert(alist_entries_seen == graph->alist_entries);
  munmap(fd_rew_index_map, graph->vertex_cnt*sizeof(unsigned long));
  munmap(fd_rew_map, graph->alist_entries*sizeof(double));
  close_csr(graph);
  close(fd_rew);
  close(fd_rew_index);
  CLOCK_STOP(time_total);
  printf("TIME TOTAL %lu\n", time_total);
  return 0;
}
