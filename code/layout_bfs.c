#include "graph_defs.h"

typedef struct bfs_metadata_st {
  char touched;
  unsigned long queue_next;
} bfs_metadata_t;

int main(int argc, char **argv)
{
  unsigned char string_buffer[1024];
  int fd_cvertices, fd_calist, fd_info;
  unsigned long i, root;
  unsigned long queue_head, queue_tail;
  unsigned long page_queue_head, page_queue_tail;
  unsigned long alist_entries_seen = 0;
  unsigned long keep_compiler_happy;
  if(argc < 4) {
    fprintf(stderr, "Usage %s graph_in root_id graph_out\n", argv[0]);
    exit(-1);
  }
  graph_t *graph = open_graph(argv[1]);
  root = atol(argv[2]);
  assert(root < graph->vertex_cnt);
  strcpy(string_buffer, argv[3]);
  strcat(string_buffer, ".vertices");
  fd_cvertices = open(string_buffer, O_RDWR|O_CREAT|O_TRUNC|O_LARGEFILE, S_IRWXU);
  if(fd_cvertices == -1) {
    perror("Unable to open vertices file for writing:");
    exit(-1);
  }
  strcpy(string_buffer, argv[3]);
  strcat(string_buffer, ".calist");
  fd_calist = open(string_buffer, O_RDWR|O_CREAT|O_TRUNC|O_LARGEFILE, S_IRWXU);
  if(fd_calist == -1) {
    perror("Unable to open calist file for writing:");
    exit(-1);
  }
  strcpy(string_buffer, argv[3]);
  strcat(string_buffer, ".info");
  fd_info = open(string_buffer, O_RDWR|O_CREAT|O_TRUNC, S_IRWXU);
  if(fd_info == -1) {
    perror("Unable to open info file for writing:");
    exit(-1);
  }
  sprintf(string_buffer, "vertices=%lu\n", graph->vertex_cnt);
  keep_compiler_happy = write(fd_info, string_buffer, strlen(string_buffer));
  sprintf(string_buffer, "alist_entries=%lu\n", graph->alist_entries);
  keep_compiler_happy = write(fd_info, string_buffer, strlen(string_buffer));
  sprintf(string_buffer, "alist_bytes=%lu\n", graph->calist_bytes);
  keep_compiler_happy = write(fd_info, string_buffer, strlen(string_buffer));
  close(fd_info);
  bfs_metadata_t * metadata = (bfs_metadata_t*)
    map_anon_memory(graph->vertex_cnt*sizeof(bfs_metadata_t), "vertex metadata");
  /* Perhaps mmap /dev/null instead ? */
  memset(metadata, 0, graph->vertex_cnt*sizeof(bfs_metadata_t));
#ifndef __APPLE__
	if(posix_fallocate(fd_cvertices, 0, graph->vertex_cnt*sizeof(vertex_t))) {
	  perror("Unable to allocate space for vertices:");
	  exit(-1);
	}
#else
	for(i=0;
	    i<(graph->vertex_cnt*sizeof(vertex_t)/sizeof(unsigned long));
	    i++) {
	  unsigned long zero = 0;
	  keep_compiler_happy = write(fd_cvertices, &zero, sizeof(unsigned long));
	}
	keep_compiler_happy = lseek(fd_cvertices, 0, SEEK_SET);
#endif
  i = root;
  unsigned long calist_current_offset = 0, size_current_calist;
  vertex_t tmp_vertex;
  do {
    if(metadata[i].touched == 0) {
      metadata[i].touched = 1;
      queue_head = queue_tail = ULONG_MAX;
      page_queue_head = page_queue_tail = ULONG_MAX;
      unsigned long consumed_page_bytes = 0;
      BFS_PUSH(queue_head, queue_tail, i, metadata);
      while((queue_head != ULONG_MAX) || (page_queue_head != ULONG_MAX)) {
	unsigned long current_vertex;
	if(page_queue_head == ULONG_MAX) {
	  current_vertex = BFS_POP(queue_head, queue_tail, metadata);
	}
	else {
	  current_vertex = BFS_POP(page_queue_head, page_queue_tail, metadata);
	}
	/* Write out information for current vertex */
	vertex_t *cur_vert = &graph->vertex_map[current_vertex];
	memcpy(&tmp_vertex, cur_vert, sizeof(vertex_t));
	tmp_vertex.offset_edges = calist_current_offset;
	tmp_vertex.offset_edge_info = calist_current_offset +
	  (cur_vert->offset_edge_info - cur_vert->offset_edges);
	keep_compiler_happy = lseek(fd_cvertices,
				    current_vertex*sizeof(vertex_t),
				    SEEK_SET);
	assert(keep_compiler_happy == current_vertex*sizeof(vertex_t));
	keep_compiler_happy = write(fd_cvertices, &tmp_vertex,
				    sizeof(vertex_t));
	assert(keep_compiler_happy == sizeof(vertex_t));
	size_current_calist = get_size_calist(graph, current_vertex);
	keep_compiler_happy = write(fd_calist,
				    &graph->calist_map[cur_vert->offset_edges],
				    size_current_calist);
	assert(keep_compiler_happy == size_current_calist);
	calist_current_offset += size_current_calist;
	edge_iterator_t iter;
	init_edge_iterator(graph, current_vertex, &iter);
	while(iter_step(graph, &iter) == 0) {
	  unsigned long target = iter.neighbour;
	  if(metadata[target].touched == 0) {
	    metadata[target].touched = 1;
	    BFS_PUSH(page_queue_head, page_queue_tail, target, metadata);
	  }
	  alist_entries_seen++;
	}
	consumed_page_bytes += size_current_calist;
	if(consumed_page_bytes >= ASSUME_PAGE_SIZE) {
	  consumed_page_bytes = 0;
	  if(page_queue_head != ULONG_MAX) { /* might have been leaves */
	    BFS_LIST_APPEND(queue_head, queue_tail, page_queue_head,
			    page_queue_tail, metadata);
	    page_queue_head = page_queue_tail = ULONG_MAX;
	  }
	}
      }
    }
    i = i + 1;
    if(i >= graph->vertex_cnt) {
      i = 0;
    }
  } while(i != root);
  assert(alist_entries_seen == graph->alist_entries);
  munmap(metadata, graph->vertex_cnt*sizeof(bfs_metadata_t));
  close(fd_cvertices);
  close(fd_calist);
  close_graph(graph);
  return 0;
}
