#include "graph_defs.h"

int main(int argc, char **argv)
{
  FILE* fp_in;
  int fd_out, fd_vertices, fd_edges;
  char string_buffer[1024];
  char string_buffer2[1024];
  unsigned long vertex_cnt, alist_entries;
  char *token;
  unsigned long i;
  uncompressed_vertex_t *vertex_map;
  unsigned long current_offset;
  unsigned long keep_compiler_happy;
  unsigned long time_pass1, time_pass2, time_pass3, time_total;

  CLOCK_START(time_total);

  if(argc < 4) {
    fprintf(stderr, "Usage %s forward_graph reverse_graph output_graph\n",
	    argv[0]);
    exit(-1);
  }
  fp_in = fopen(argv[1], "r");
  if(fp_in == NULL) {
    perror("Unable to open input file:");
    exit(-1);
  }
  assert(strlen(argv[3]) < (1023 - 10));
  strcpy(string_buffer, argv[3]);
  strcat(string_buffer, ".info");
  fd_out = open(string_buffer, O_RDWR|O_CREAT|O_TRUNC, S_IRWXU);
  if(fd_out == -1) {
    perror("Unable to open info file:");
    exit(-1);
  }
  fprintf(stderr, "Pass 1...writing vertex info\n");
  CLOCK_START(time_pass1);
  vertex_cnt = 0;
  alist_entries = 0;
  unsigned long last_from = ULONG_MAX, last_to = ULONG_MAX;
  unsigned long eindex;
  while(fgets(string_buffer, 1024, fp_in) != NULL) {
    if(string_buffer[0] == '#') {
      token = strtok(string_buffer, " ");
      token = strtok(NULL, " ");
      if(strcmp(token, "Nodes:") == 0) {
	vertex_cnt = atol(strtok(NULL, " "));
	strcpy(string_buffer2, argv[3]);
	strcat(string_buffer2, ".vertices");
	fd_vertices = open(string_buffer2, O_RDWR|O_CREAT|O_TRUNC|O_LARGEFILE, S_IRWXU);
	if(fd_vertices == -1) {
	  perror("Unable to open vertices file for writing:");
	  exit(-1);
	}
#ifndef __APPLE__
	if(posix_fallocate(fd_vertices, 0, vertex_cnt*sizeof(uncompressed_vertex_t))) {
	  perror("Unable to allocate space for vertices:");
	  exit(-1);
	}
#else
	for(i=0;
	    i<(vertex_cnt*sizeof(uncompressed_vertex_t)/sizeof(unsigned long));
	    i++) {
	  unsigned long zero = 0;
	  keep_compiler_happy = write(fd_vertices, &zero, sizeof(unsigned long));
	}
	keep_compiler_happy = lseek(fd_vertices, 0, SEEK_SET);
#endif
	vertex_map =
	  (uncompressed_vertex_t *)mmap(0, vertex_cnt*sizeof(uncompressed_vertex_t),
			   PROT_READ|PROT_WRITE, MAP_FILE|MAP_SHARED,
			   fd_vertices, 0);
	if(vertex_map == MAP_FAILED) {
	  perror("Unable to map in vertices:");
	  exit(-1);
	}
	if(mlock(vertex_map, vertex_cnt * sizeof(uncompressed_vertex_t)) < 0) {
	  perror("mlocking vertex map failed:");
	}
	memset(vertex_map, 0, vertex_cnt*sizeof(uncompressed_vertex_t));
      }
    }
    else {
      unsigned long from = atol(strtok(string_buffer," \t"));
      unsigned long to = atol(strtok(NULL," \t"));
      //fprintf(stderr, "%lu -> %lu\n", from, to); 
      assert(from < vertex_cnt);
      assert(to < vertex_cnt);
      if(from != to) {
	if((last_from != from) || (last_to != to)) {
	  alist_entries++;
	  vertex_map[from].degree++;
	  vertex_map[to].degree++;
	}
	last_from = from;
	last_to = to;
      }
    }
  }
  CLOCK_STOP(time_pass1);  
  sprintf(string_buffer, "vertices=%lu\n", vertex_cnt);
  keep_compiler_happy = write(fd_out, string_buffer, strlen(string_buffer));
  alist_entries <<=1; /* out and in edges represented */
  sprintf(string_buffer, "alist_entries=%lu\n", alist_entries);
  keep_compiler_happy = write(fd_out, string_buffer, strlen(string_buffer));
  rewind(fp_in);
  strcpy(string_buffer, argv[3]);
  strcat(string_buffer, ".temp");
  fd_edges = open(string_buffer, O_RDWR|O_CREAT|O_TRUNC|O_LARGEFILE, S_IRWXU);
  if(fd_edges == -1) {
    perror("Unable to open edges file for writing:");
    exit(-1);
  }
  current_offset = 0;
  for(i=0;i<vertex_cnt;i++) {
    vertex_map[i].offset_edges = current_offset;
    current_offset += vertex_map[i].degree*sizeof(unsigned long);
    vertex_map[i].offset_edge_info = current_offset;
    current_offset += ROUND_BYTE(vertex_map[i].degree);
    vertex_map[i].degree = 0;
  }
#ifndef __APPLE__
  if(posix_fallocate(fd_edges, 0, current_offset)) {
    perror("Unable to allocate space for edges:");
    exit(-1);
  }
#else
  for(i=0;i<current_offset/sizeof(unsigned long);i++) {
    unsigned long zero = 0;
    keep_compiler_happy = write(fd_edges, &zero, sizeof(unsigned long));
  }
#endif
  char *uncomp_alist_map = 
    mmap(0, current_offset,
	 PROT_READ|PROT_WRITE, MAP_FILE|MAP_SHARED,
	 fd_edges, 0);
  if(uncomp_alist_map == MAP_FAILED) {
    perror("Unable to map in edges:");
    exit(-1);
  }
  sprintf(string_buffer, "alist_bytes=%lu\n", current_offset);
  keep_compiler_happy = write(fd_out, string_buffer, strlen(string_buffer));
  unsigned long target = 1000000;
  unsigned long clock_target;
  fprintf(stderr, "Pass 2...writing uncompressed forward edge list\n");
  CLOCK_START(time_pass2);
  CLOCK_START(clock_target);
  last_from = ULONG_MAX;
  last_to = ULONG_MAX;
  unsigned long sticky_forward = 0; /* false */
  while(fgets(string_buffer, 1024, fp_in) != NULL) {
    if(target == 0) {
      CLOCK_STOP(clock_target);
      fprintf(stderr,"Wrote 1000000 edges, time %lu\n", clock_target);
      target = 1000000;
      CLOCK_START(clock_target);
    }
    if(string_buffer[0] != '#') {
      unsigned long from = atol(strtok(string_buffer," \t"));
      unsigned long to = atol(strtok(NULL," \t"));
      char dir = strtok(NULL, " \t")[0];
      assert(from < vertex_cnt);
      assert(to < vertex_cnt);
      if(from == to) {
	continue;
      }
      if((from != last_from) || (to != last_to)) {
	sticky_forward = 0; /* reset */
	eindex = vertex_map[from].degree;
      }
      else {
	assert(vertex_map[from].degree > 0);
	eindex = vertex_map[from].degree - 1;
      }
      unsigned long* alist = (unsigned long *)
	&uncomp_alist_map[vertex_map[from].offset_edges];
      unsigned char *props = &uncomp_alist_map[vertex_map[from].offset_edge_info];
      if(dir == 'F') {
	/* Unset bit for outgoing edge */
	props[BYTE_INDEX(eindex)] &=
	  ~((unsigned char)1 << BIT_INDEX(eindex));
	sticky_forward = 1; /* outgoing edge has priority */
      }
      else if(dir == 'R'){
	/* set bit for incoming edge */
	if(!sticky_forward) {
	  props[BYTE_INDEX(eindex)] |=
	    ((unsigned char)1 << BIT_INDEX(eindex));
	}
      }
      else {
	fprintf(stderr, "Unrecognized direction %c\n", dir);
	exit(-1);
      }
      if((last_from != from) || (last_to != to)) {
	alist[vertex_map[from].degree++] = to;
      }
      last_from = from;
      last_to = to;
      target--;
    }
  }
  CLOCK_STOP(time_pass2);
  fclose(fp_in);
  fp_in = fopen(argv[2], "r");
  if(fp_in == NULL) {
    perror("Unable to open input file:");
    exit(-1);
  }
  fprintf(stderr, "Pass 3...writing uncompressed reverse edge list\n");
  target = 1000000;
  CLOCK_START(time_pass3);
  CLOCK_START(clock_target);
  last_from = ULONG_MAX;
  last_to = ULONG_MAX;
  unsigned long sticky_reverse = 0;
  while(fgets(string_buffer, 1024, fp_in) != NULL) {
    if(target == 0) {
      CLOCK_STOP(clock_target);
      fprintf(stderr, "Wrote 1000000 edges, time %lu\n", clock_target);
      target = 1000000;
      CLOCK_START(clock_target);
    }
    if(string_buffer[0] != '#') {
      unsigned long from = atol(strtok(string_buffer," \t"));
      unsigned long to = atol(strtok(NULL," \t"));
      char dir = strtok(NULL, " \t")[0];
      assert(from < vertex_cnt);
      assert(to < vertex_cnt);
      if(from == to) {
	continue;
      }
      if((from != last_from) || (to != last_to)) {
	sticky_reverse = 0; /* reset */
	eindex = vertex_map[to].degree;
      }
      else {
	assert(vertex_map[to].degree > 0);
	eindex = vertex_map[to].degree - 1;
      }
      unsigned long *alist = (unsigned long *)
	&uncomp_alist_map[vertex_map[to].offset_edges];
      unsigned char* props = &uncomp_alist_map[vertex_map[to].offset_edge_info];
      if(dir == 'R') {
	/* unset bit for outgoing edge */
	props[BYTE_INDEX(eindex)] &=
	  ~(1 << BIT_INDEX(eindex));
	sticky_reverse = 1; /* outgoing edge has priority */
      }
      else if(dir == 'F') {
	if(!sticky_reverse) {
	  props[BYTE_INDEX(eindex)] |=
	    (1 << BIT_INDEX(eindex));
	}
      }
      else {
	fprintf(stderr, "Unrecognized direction %c\n", dir);
	exit(-1);
      }
      if((from != last_from) || (to != last_to)) {
	alist[vertex_map[to].degree++] = from;
      }
      last_from = from;
      last_to = to;
      target--;
    }
  }
  CLOCK_STOP(time_pass3);
  
  fclose(fp_in);
  close(fd_out);
  close(fd_vertices);
  close(fd_edges);
  CLOCK_STOP(time_total);
  printf("TIME PASS1 %lu\n", time_pass1);
  printf("TIME PASS2 %lu\n", time_pass2);
  printf("TIME PASS3 %lu\n", time_pass3);
  printf("TIME TOTAL %lu\n", time_total);
  return 0;
}
