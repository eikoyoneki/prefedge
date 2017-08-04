#include "graph_defs.h"

static graph_t * volatile graph;

/* Randomise map */
void randomise_map(unsigned long *fwd_map,
		   unsigned long *inv_map,
		   unsigned long vertex_cnt)
{
  unsigned long i, interchange_pos, temp;
  srand48(0xdeadbeef);
  fwd_map[0] = 0;
  fwd_map[1] = 1;
  /* Randomly interchange remap */
  for(i=2;i<vertex_cnt;i++) {
    fwd_map[i] = i;
    interchange_pos = 1 + (lrand48() % (i));
    assert(interchange_pos <= i);
    assert(interchange_pos >= 1);
    fwd_map[i]=fwd_map[interchange_pos];
    fwd_map[interchange_pos]= i;
  }
  for(i=0;i<vertex_cnt;i++) {
    inv_map[fwd_map[i]] = i;
  }
}


void encode(__uint128_t value,
	    unsigned long bits, 
	    unsigned char *byte_stream,
	    unsigned long bit_offset)
{
  value <<= bit_offset;
  value |= (byte_stream[0] & ((1 << bit_offset) - 1));
  memcpy(byte_stream, &value, ( bits + bit_offset + 7)/8);
}

void emit_rew_index(unsigned long* rew_index_in,
		    int fd_rew_index_out,
		    unsigned long *inv_map)
{
  unsigned long i;
  unsigned long target = 1000000, clock_target;
  unsigned long keep_compiler_happy;
  CLOCK_START(clock_target);
  for(i=0; i<graph->vertex_cnt; i++) {
    keep_compiler_happy = write(fd_rew_index_out, 
				&rew_index_in[inv_map[i]], 
				sizeof(unsigned long));
    target--;
    if(target == 0) {
      CLOCK_STOP(clock_target);
      fprintf(stderr,"Wrote rew index 1000000 vertices, time %lu\n",
		    clock_target);
      target = 1000000;
      CLOCK_START(clock_target);
    }
  }
}

void convert(unsigned long* index, 
	     unsigned char *byte_stream, 
	     unsigned long bits,
	     unsigned long *fwd_map,
	     unsigned long *inv_map)
{
  unsigned long i, offset = 0;
  unsigned long max_vertex = graph->vertex_cnt - 1;
  unsigned long target = 1000000;
  unsigned long clock_target;
  unsigned long degree, keep_compiler_happy;
  CLOCK_START(clock_target);
  for(i=0; i<graph->vertex_cnt; i++) {
    index[i]=offset;
    edge_iterator_t iter;
    init_edge_iterator(graph, inv_map[i], &iter);
    degree = 0;
    while(iter_step(graph, &iter) == 0) {
      __uint128_t value = fwd_map[iter.neighbour];
      value = value << 1;
      if(iter.incoming) {
	value |= 1;
      }
      // Emit CSR-opt
      encode(value, bits, byte_stream +
	     BYTE_INDEX(offset), BIT_INDEX(offset));
      offset += bits;
      target--;
      degree++;
      if(target == 0) {
	CLOCK_STOP(clock_target);
	fprintf(stderr,"Wrote 1000000 edges, time %lu\n", clock_target);
	target = 1000000;
	CLOCK_START(clock_target);
      }
    }
  }
}

int main(int argc, char **argv)
{
  unsigned long time_total;
  CLOCK_START(time_total);
  if(argc < 3) {
    fprintf(stderr, "Usage %s graph_name_in graph_name_out\n", argv[0]);
    exit(-1);
  }
  graph = open_vertices(argv[1]);
  open_cal(graph);
  unsigned long bits=0;
  unsigned long max_vertex = graph->vertex_cnt - 1;
  do {
    bits++;
  } while(max_vertex=(max_vertex>>1));
  bits++; /* dirn */
  char string_buffer[1024];
  strcpy(string_buffer, argv[2]);
  strcat(string_buffer, ".vertices");
  int fd_index = open(string_buffer, 
		      O_RDWR|O_CREAT|O_TRUNC|O_LARGEFILE,
		      S_IRWXU);
  if(fd_index == -1) {
    perror("Unable to open vertices file for writing:");
    exit(-1);
  }
  if(posix_fallocate(fd_index, 0, 
		     graph->vertex_cnt*
		     sizeof(unsigned long))) {
    perror("Unable to allocate space for vertices:");
    exit(-1);
  }
  unsigned long *index = (unsigned long*) 
    mmap(0, 
	 graph->vertex_cnt*sizeof(unsigned long),
	 PROT_READ|PROT_WRITE, MAP_FILE|MAP_SHARED,
	 fd_index, 0);
  if(index == MAP_FAILED) {
    perror("Unable to map in vertices:");
    exit(-1);
  }
  memset(index, 
	 0, 
	 graph->vertex_cnt*sizeof(unsigned long));
  strcpy(string_buffer, argv[2]);
  strcat(string_buffer, ".calist");
  int fd_edges = open(string_buffer, 
		  O_RDWR|O_CREAT|O_TRUNC|O_LARGEFILE, 
		  S_IRWXU);
  if(fd_edges == -1) {
    perror("Unable to open edges file for writing:");
    exit(-1);
  }
  unsigned long alist_bytes = 
    BYTE_INDEX(graph->alist_entries*bits) + 1;
  if(posix_fallocate(fd_edges, 0, alist_bytes)) {
    perror("Unable to allocate space for edges:");
    exit(-1);
  }
  unsigned char *byte_stream = 
    mmap(0, alist_bytes,
	 PROT_READ|PROT_WRITE, MAP_FILE|MAP_SHARED,
	 fd_edges, 0);
  if(byte_stream == MAP_FAILED) {
    perror("Unable to map in edges:");
    exit(-1);
  }
  unsigned long *fwd_map = 
    map_anon_memory(graph->vertex_cnt * sizeof(unsigned long),
		    "fwd map");
  unsigned long *inv_map = 
    map_anon_memory(graph->vertex_cnt * sizeof(unsigned long),
		    "inv map");
  fprintf(stderr, "Randomising map...\n");
  randomise_map(fwd_map, inv_map, graph->vertex_cnt);
  fprintf(stderr, "Done\n");
  strcpy(string_buffer, argv[2]);
  strcat(string_buffer, ".rew_index");
  int fd_rew_index = open(string_buffer, 
			  O_RDWR|O_CREAT|O_TRUNC|O_LARGEFILE,
			  S_IRWXU);
  if(fd_rew_index == -1) {
    perror("Unable to open rew index file for writing:");
    exit(-1);
  }
  strcpy(string_buffer, argv[1]);
  strcat(string_buffer, ".rew_index");
  int fd_rew_index_in = open(string_buffer, 
			     O_RDONLY|O_LARGEFILE);
  if(fd_rew_index_in == -1) {
    perror("Unable to open input rew index file for reading:");
    exit(-1);
  }
  unsigned long *index_in = (unsigned long *)
    mmap(0,  (graph->vertex_cnt)*sizeof(unsigned long),
	 PROT_READ, MAP_SHARED|MAP_FILE, fd_rew_index_in, 0);
  if(index_in == MAP_FAILED) {
    perror("Unable to map input rew index:");
    exit(-1);
  }
  fprintf(stderr, "Writing new edge list...\n");
  convert(index, byte_stream, bits, fwd_map, inv_map);  
  fprintf(stderr, "Done\n");
  fprintf(stderr, "Writing new rew index...\n");
  emit_rew_index(index_in, fd_rew_index, inv_map);  
  fprintf(stderr, "Done\n");
  close_graph(graph);
  close(fd_index);
  close(fd_edges);
  close(fd_rew_index);
  strcpy(string_buffer, argv[2]);
  strcat(string_buffer, ".info");
  int fd_info = open(string_buffer, 
		     O_RDWR|O_CREAT|O_TRUNC, 
		     S_IRWXU);
  if(fd_info == -1) {
    perror("Unable to open info file:");
    exit(-1);
  }
  sprintf(string_buffer, "vertices=%lu\n", 
	  graph->vertex_cnt);
  unsigned long keep_compiler_happy;
  keep_compiler_happy = write(fd_info, 
			      string_buffer, 
			      strlen(string_buffer));
  sprintf(string_buffer, "alist_entries=%lu\n", 
	  graph->alist_entries);
  keep_compiler_happy = write(fd_info, 
			      string_buffer, 
			      strlen(string_buffer));
  sprintf(string_buffer, 
	  "alist_bytes=%lu\n", 
	  alist_bytes);
  keep_compiler_happy = write(fd_info, 
			      string_buffer, 
			      strlen(string_buffer));
  close(fd_rew_index);
  close(fd_rew_index_in);
  close(fd_info);
  CLOCK_STOP(time_total);
  printf("TIME TOTAL %lu\n", time_total);
  return 0;
}
