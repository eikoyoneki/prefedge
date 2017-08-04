#include "graph_defs.h"

void encode(unsigned long neighbour,
	    unsigned long incoming,
	    unsigned long bits, 
	    unsigned char *byte_stream,
	    unsigned long bit_offset)
{
  __uint128_t value = neighbour;
  value = value << 1;
  if(incoming) {
    value |= 1;
  }
  value <<= bit_offset;
  value |= (byte_stream[0] & ((1 << bit_offset) - 1));
  memcpy(byte_stream, &value, ( bits + bit_offset + 7)/8);
}


int main(int argc, char **argv)
{
  FILE* fp_in;
  int fd_out, fd_vertices, fd_edges;
  char string_buffer[1024];
  char string_buffer2[1024];
  unsigned long vertex_cnt, alist_entries;
  char *token;
  unsigned long i;
  unsigned long *offsets;
  unsigned long *degrees;
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
	if(posix_fallocate(fd_vertices, 0, vertex_cnt*sizeof(unsigned long))) {
	  perror("Unable to allocate space for vertices:");
	  exit(-1);
	}
	offsets =
	  (unsigned long *)mmap(0, vertex_cnt*sizeof(unsigned long),
			   PROT_READ|PROT_WRITE, MAP_FILE|MAP_SHARED,
			   fd_vertices, 0);
	if(offsets == MAP_FAILED) {
	  perror("Unable to map in vertices:");
	  exit(-1);
	}
	if(mlock(offsets, vertex_cnt * sizeof(unsigned long)) < 0) {
	  perror("mlocking vertex offsets failed:");
	}
	memset(offsets, 0, vertex_cnt*sizeof(unsigned long));
	degrees =
	  (unsigned long *)mmap(0, vertex_cnt*sizeof(unsigned long),
			   PROT_READ|PROT_WRITE, MAP_ANONYMOUS|MAP_SHARED,
			   -1, 0);
	if(degrees == MAP_FAILED) {
	  perror("Unable to map in degrees:");
	  exit(-1);
	}
	if(mlock(degrees, vertex_cnt * sizeof(unsigned long)) < 0) {
	  perror("mlocking degree map failed:");
	}
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
	  degrees[from]++;
	  degrees[to]++;
	}
	last_from = from;
	last_to = to;
      }
    }
  }
  unsigned long bits=0;
  unsigned long max_vertex = vertex_cnt - 1;
  do {
    bits++;
  } while(max_vertex=(max_vertex>>1));
  bits++; /* dirn */
  CLOCK_STOP(time_pass1);  
  sprintf(string_buffer, "vertices=%lu\n", vertex_cnt);
  keep_compiler_happy = write(fd_out, string_buffer, strlen(string_buffer));
  alist_entries <<=1; /* out and in edges represented */
  sprintf(string_buffer, "alist_entries=%lu\n", alist_entries);
  keep_compiler_happy = write(fd_out, string_buffer, strlen(string_buffer));
  rewind(fp_in);
  strcpy(string_buffer, argv[3]);
  strcat(string_buffer, ".calist");
  fd_edges = open(string_buffer, O_RDWR|O_CREAT|O_TRUNC|O_LARGEFILE, S_IRWXU);
  if(fd_edges == -1) {
    perror("Unable to open edges file for writing:");
    exit(-1);
  }
  current_offset = 0;
  for(i=0;i<vertex_cnt;i++) {
    offsets[i] = current_offset;
    current_offset += degrees[i]*bits;
    degrees[i] = 0;
  }
  unsigned long alist_bytes = (current_offset + 7)/8;
  if(posix_fallocate(fd_edges, 0, alist_bytes)) {
    perror("Unable to allocate space for edges:");
    exit(-1);
  }
  char *uncomp_alist_map = 
    mmap(0, alist_bytes,
	 PROT_READ|PROT_WRITE, MAP_FILE|MAP_SHARED,
	 fd_edges, 0);
  if(uncomp_alist_map == MAP_FAILED) {
    perror("Unable to map in edges:");
    exit(-1);
  }
  sprintf(string_buffer, "alist_bytes=%lu\n", alist_bytes);
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
	eindex = degrees[from];
      }
      else {
	assert(degrees[from] > 0);
	eindex = degrees[from] - 1;
      }
      unsigned char* alist = (unsigned char *)
	&uncomp_alist_map[BYTE_INDEX(offsets[from]+eindex*bits)];
      unsigned long incoming = 0;
      if(dir == 'F') {
	/* Unset bit for outgoing edge */
	sticky_forward = 1; /* outgoing edge has priority */
      }
      else if(dir == 'R'){
	/* set bit for incoming edge */
	if(!sticky_forward) {
	  incoming = 1;
	}
      }
      else {
	fprintf(stderr, "Unrecognized direction %c\n", dir);
	exit(-1);
      }
      if((last_from != from) || (last_to != to)) {
	degrees[from]++;
      }
      encode(to, incoming, bits, alist, 
	     BIT_INDEX(offsets[from] + eindex*bits)); // Idempotent
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
	eindex = degrees[to];
      }
      else {
	assert(degrees[to] > 0);
	eindex = degrees[to] - 1;
      }
      unsigned char* alist = (unsigned char *)
	&uncomp_alist_map[BYTE_INDEX(offsets[to]+eindex*bits)];
      unsigned long incoming = 0;
      if(dir == 'R') {
	/* unset bit for outgoing edge */
	sticky_reverse = 1; /* outgoing edge has priority */
      }
      else if(dir == 'F') {
	if(!sticky_reverse) {
	  incoming = 1;
	}
      }
      else {
	fprintf(stderr, "Unrecognized direction %c\n", dir);
	exit(-1);
      }
      if((from != last_from) || (to != last_to)) {
	degrees[to]++;
      }
      encode(from, incoming, bits, alist, 
	     BIT_INDEX(offsets[to] + eindex*bits)); // Idempotent
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
