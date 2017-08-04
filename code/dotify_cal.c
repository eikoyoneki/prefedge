#include "graph_defs.h"

int main(int argc, char **argv)
{
  unsigned long i, alist_entries_seen = 0, keep_compiler_happy;
  char string_buffer[1024];
  int fd_dot;
  if(argc < 2) {
    fprintf(stderr, "Usage %s graph_name\n", argv[0]);
    exit(-1);
  }
  graph_t *graph = open_graph(argv[1]);
  assert(strlen(argv[1]) < (1023 - 10));
  strcpy(string_buffer, argv[1]);
  strcat(string_buffer, ".gv");
  fd_dot = open(string_buffer, O_RDWR|O_CREAT|O_TRUNC|O_LARGEFILE, S_IRWXU);
  if(fd_dot == -1) {
    perror("Unable to open dot output file:");
    exit(-1);
  }
  snprintf(string_buffer, 1023, "digraph %s {\n", argv[1]); 
  keep_compiler_happy = write(fd_dot, string_buffer, strlen(string_buffer));
  assert(keep_compiler_happy == strlen(string_buffer));
  for(i=0;i<graph->vertex_cnt;i++) {
    edge_iterator_t iter;
    init_edge_iterator(graph, i, &iter);
    while(iter_step(graph, &iter) == 0) {
      if(!iter.incoming) {
	snprintf(string_buffer, 1023, "\t%lu -> %lu;\n", 
		 i, iter.neighbour);
	keep_compiler_happy = write(fd_dot, string_buffer, strlen(string_buffer));
	assert(keep_compiler_happy == strlen(string_buffer));
      }
      alist_entries_seen++;
    }
  }
  snprintf(string_buffer, 1023, "}\n"); 
  keep_compiler_happy = write(fd_dot, string_buffer, strlen(string_buffer));
  assert(keep_compiler_happy == strlen(string_buffer));
  close(fd_dot);
  assert(graph->alist_entries == alist_entries_seen);
  close_graph(graph);
  return 0;
}
