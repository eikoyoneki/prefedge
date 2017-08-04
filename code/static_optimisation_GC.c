#include "graph_defs.h"

#ifndef DIMACS
typedef uncompressed_vertex_t uncompressed_vertex_type;
#else
typedef uncompressed_dimacs_vertex_t uncompressed_vertex_type;
#endif

typedef struct dfs_metadata_st {
  char touched;
  unsigned long parent;
  unsigned long children;
  unsigned long child_next;
  unsigned long stack_next;
  unsigned long next_edge;
  unsigned long weight; /* nodes in dfs subtree */
  unsigned long remap_position;
} dfs_metadata_t;

#define SATURATING_SUBTRACT(_tgt, _amt)		\
  do {						\
  if((_tgt) < (_amt)) {				\
    (_tgt) = 0;					\
  }						\
  else {					\
    (_tgt) -= (_amt);				\
  }} while(0)					

/* Returns a forest of DFS trees -- O(V + E) */
unsigned long build_dfs_tree(dfs_metadata_t *dfs_metadata, 
			     uncompressed_vertex_type *vertex_map,
			     char *alist_map,
			     unsigned long vertex_cnt,
			     unsigned long alist_entries)
{
  unsigned long top = ULONG_MAX;
  unsigned long connected_components = 0;
  unsigned long considered_alist_entries = 0;
  unsigned long i;
  unsigned long dfs_tree_roots = ULONG_MAX;
  assert(vertex_cnt < ULONG_MAX);
  unsigned long periodic_clock;
  unsigned long target_edges = 1000000;
  CLOCK_START(periodic_clock);
  for(i=0;i<vertex_cnt;i++) {
    if(dfs_metadata[i].touched == 0) {
      /* Untouched vertex, new dfs root */
      dfs_metadata[i].touched = 1;
      dfs_metadata[i].parent = ULONG_MAX;
      dfs_metadata[i].children = ULONG_MAX;
      dfs_metadata[i].next_edge = 0;
      /* Add to DFS tree roots -- overload next child */
      dfs_metadata[i].child_next = dfs_tree_roots;
      dfs_tree_roots = i;
      connected_components++;
      DFS_PUSH(top, i, dfs_metadata);
      while(top != ULONG_MAX) {
	unsigned long consider_edge = dfs_metadata[top].next_edge++;
	if(vertex_map[top].degree > consider_edge) {
	  unsigned long *alist = (unsigned long *)&alist_map[vertex_map[top].offset_edges];
	  unsigned long target = alist[consider_edge];
	  assert(target < vertex_cnt);
	  assert(target != top);
	  considered_alist_entries++;
	  target_edges--;
	  if(target_edges == 0) {
	    CLOCK_STOP(periodic_clock);
	    fprintf(stderr, "DFS saw 1000000 edges time %lu\n", periodic_clock);
	    CLOCK_START(periodic_clock);
	    target_edges = 1000000;
	  }
	  if(dfs_metadata[target].touched != 1) {
	    dfs_metadata[target].touched = 1;
	    dfs_metadata[target].parent = top;
	    dfs_metadata[target].children = ULONG_MAX;
	    dfs_metadata[target].next_edge = 0;
	    dfs_metadata[target].child_next = dfs_metadata[top].children;
	    dfs_metadata[top].children = target;
	    DFS_PUSH(top, target, dfs_metadata);
	  } 
	}
	else {
	  dfs_metadata[top].weight++; /* Account for itself */
	  if(dfs_metadata[top].parent != ULONG_MAX) {
	    dfs_metadata[dfs_metadata[top].parent].weight +=
	      dfs_metadata[top].weight; /* propagate weight information */
	  }
	  DFS_POP(top, dfs_metadata);
	}
      }
    }
  }
  assert(considered_alist_entries == alist_entries);
  fprintf(stderr, "\n%lu DFS trees created from %lu vertices\n",
	  connected_components, vertex_cnt);
  return dfs_tree_roots;
}

/* Heuristic remap to reduce CAL sizes -- O(V) */
void perform_remap(unsigned long forest,
		   dfs_metadata_t *dfs_metadata, 
		   uncompressed_vertex_type *vertex_map,
		   unsigned long vertex_cnt)
{
  unsigned long next_avail = 0, temp;
  while(forest != ULONG_MAX) {
    unsigned long root = forest;
    forest = dfs_metadata[root].child_next;
    while(root != ULONG_MAX) {
      if(dfs_metadata[root].touched == 1) { /* Seen for the first time        */
	dfs_metadata[root].touched = 0;
	dfs_metadata[root].remap_position = ULONG_MAX;
	if(dfs_metadata[root].parent != ULONG_MAX) {
	  /* Account for this subtree */
	  SATURATING_SUBTRACT(dfs_metadata[dfs_metadata[root].parent].weight, \
			      dfs_metadata[root].weight); 
	}
	dfs_metadata[root].weight /= 2;    /* Target for enumerating the root */
      }
      if(dfs_metadata[root].remap_position == ULONG_MAX) { /* Enumerate ? */
	if(dfs_metadata[root].weight == 0 ||
	   dfs_metadata[root].children == ULONG_MAX) { /* enumerate now */
	  dfs_metadata[root].remap_position = (next_avail++);
	  /* overload stack_next for inverse map */
	  dfs_metadata[dfs_metadata[root].remap_position].stack_next = root; 
	}
      }
      if(dfs_metadata[root].children != ULONG_MAX) { /* move down */
	temp = dfs_metadata[root].children;
	dfs_metadata[root].children = dfs_metadata[temp].child_next;
	root = temp;
      }
      else { /* move up */
	root = dfs_metadata[root].parent;
      }
    }
  }
  assert(next_avail == vertex_cnt);
}

/* Setup an identity mapping */
void identity_remap(unsigned long forest,
		    dfs_metadata_t *dfs_metadata, 
		    uncompressed_vertex_type *vertex_map,
		    unsigned long vertex_cnt)
{
  unsigned long i;
  for(i=0;i<vertex_cnt;i++) {
    dfs_metadata[i].remap_position = i;
    dfs_metadata[i].stack_next = i;
  }
}

/* Randomise a subset of the remap */
void randomise_remap(dfs_metadata_t *dfs_metadata, 
		     unsigned long vertex_cnt,
		     unsigned long start_random)
{
  unsigned long i, interchange_pos, temp;
  srand48(0xdeadbeef);
  /* Randomly interchange remap */
  for(i=start_random+1;i<vertex_cnt;i++) {
    interchange_pos = start_random + 
      (lrand48() % (i - start_random + 1));
    assert(interchange_pos <= i);
    assert(interchange_pos >= start_random);
    temp = dfs_metadata[i].remap_position;
    dfs_metadata[i].remap_position =
      dfs_metadata[interchange_pos].remap_position;
    dfs_metadata[interchange_pos].remap_position = temp;
  }
  /* Update inverse maps */
  for(i=start_random;i<vertex_cnt;i++) {
    dfs_metadata[dfs_metadata[i].remap_position].stack_next = i;
  }
}

unsigned long calculate_bits_needed(unsigned long delta)
{
  unsigned long bits_needed = 0;
  while(delta) {
    bits_needed++;
    delta>>=1;
  }
  return bits_needed;
}

/* Buffer used to write out cal -- monotonically grows on demand */
unsigned char * cal_buffer = NULL;
unsigned long cal_buffer_size = 0;

void ensure_cal_buffer(unsigned long size)
{
  if(size > cal_buffer_size) {
    if(cal_buffer != NULL) {
      munmap(cal_buffer, cal_buffer_size);
    }
    cal_buffer = (char *)map_anon_memory(size, "cal buffer");
    cal_buffer_size = size;
  }
}

void terminate_cal_buffer()
{
  if(cal_buffer != NULL) {
    munmap(cal_buffer, cal_buffer_size);
    cal_buffer = NULL;
    cal_buffer_size = 0;
  }
}

/* Returns number of bits that need to be written */
unsigned long write_cal(unsigned long me_remapped, /* The remapped one ! */
			unsigned long degree,
			vertex_t *vertex, 
			dfs_metadata_t *dfs_metadata,
			unsigned long *alist,
			char *alist_prop,
			unsigned char base_byte,
			unsigned long base_offset)
{
  unsigned long cal_entries[65]; /* CAL entries of each possible length first
				    one is dummy */
  unsigned long cal_prop_index[65]; /* Index of next CAL entry in props*/  
  unsigned long i, target, delta, base, last_entry, j;
  int sign, cal_entry_bits;
  unsigned long total_cal_bits = 0;
  unsigned long total_bits = 0;
  unsigned long prop_index;
  assert((base_byte >> base_offset) == 0);
  if(degree == 0) {
    vertex->offset_edge_info = vertex->offset_edges;
    return 0; /* Isolated vertex, no edge info */
  }
  for(i=0;i<65;i++) {
    cal_entries[i] = 0;
    cal_prop_index[i]=0;
  }
  /* Currently use 3 passes, more efficient implementation possible but
     unnecessary if IO bound */
  for(i=0;i<degree;i++) {
    target = alist[i];
    target = dfs_metadata[target].remap_position; /* use the remapped one */
    delta = ABS_DIFF(me_remapped, target);
    assert(delta != 0); /* Self loops not allowed */
    cal_entry_bits = calculate_bits_needed(delta);
    assert(cal_entry_bits < 65);
    cal_entries[cal_entry_bits]++;
    total_cal_bits += cal_entry_bits;
  }
  base = 0;
  total_bits = total_cal_bits + 64;   /* +64 for max separator */
  unsigned long prop_bits = 2*degree; /* 1 bit for sign,
					 other for direction */
  ensure_cal_buffer(ROUND_BYTE(total_bits + prop_bits + base_offset)); 
  for(i=1;i<=64;i++) {
    if(i < 64) {
      cal_prop_index[i+1] += (cal_prop_index[i] + cal_entries[i]);
    }
    delta = cal_entries[i]*i; /* i bits per-entry                    */
    cal_entries[i] = base;    /* cal_entries overloaded to hold base */
    base += delta;
    if(delta != 0) {
      last_entry = base;
    }
    if(i < 64) {               /* not for last set of cal entries */
      base++;                  /* zero bit */
    }
  }
  total_bits = last_entry;     
  memset(cal_buffer, 0, ROUND_BYTE(base_offset + total_bits + prop_bits)); 
  /* Also sets separator bits to zero */
  vertex->offset_edge_info = vertex->offset_edges + total_bits;
  cal_buffer[0] = base_byte;
  for(i=0;i<degree;i++) {
    target = alist[i];
    target = dfs_metadata[target].remap_position; /* use the remapped one */
    delta = ABS_DIFF(me_remapped, target);
    sign = SIGN_DIFF(me_remapped, target);
    cal_entry_bits = calculate_bits_needed(delta);
    /* ASSUMES LITTLE ENDIAN !!!!!! */
    unsigned long cal_edge_offset = cal_entries[cal_entry_bits];
    assert((cal_edge_offset + cal_entry_bits) <= total_bits);
    /* bit twizzle to ensure leading bit is non-zero */
    /* We know the top bit must be non-zero */
    /* Make that the lsb that gets written out first in little-endian */
    if(cal_entry_bits > 1) {
      assert(delta & (1UL << (cal_entry_bits - 1)));
      delta = delta & ((1UL << (cal_entry_bits - 1)) - 1);
      delta = delta  << 1;
      delta = delta | 1UL;
    }
    __uint128_t tmp;
    __uint128_t tmp2;
    unsigned long start_byte = BYTE_INDEX(base_offset + cal_edge_offset);
    unsigned long stop_byte = BYTE_INDEX(base_offset + cal_edge_offset + cal_entry_bits - 1);
    memcpy(&tmp, &cal_buffer[start_byte], (stop_byte - start_byte) + 1);
    tmp2 = delta;
    tmp2 <<= BIT_INDEX(base_offset + cal_edge_offset);
    tmp |= tmp2;
    memcpy(&cal_buffer[start_byte], &tmp, (stop_byte - start_byte) + 1);
    cal_entries[cal_entry_bits] += cal_entry_bits;
    if(sign) {
      prop_index = base_offset + total_bits + POS_SIGN +
	2*cal_prop_index[cal_entry_bits];
      cal_buffer[BYTE_INDEX(prop_index)] |= 
	(1 << BIT_INDEX(prop_index)); 
    }
    if(alist_prop[BYTE_INDEX(i)] & (1 << BIT_INDEX(i))) {
      prop_index = base_offset + total_bits + POS_DIR +
	2*cal_prop_index[cal_entry_bits];
      cal_buffer[BYTE_INDEX(prop_index)] |= 
	(1 << BIT_INDEX(prop_index)); 
    }
    cal_prop_index[cal_entry_bits]++;
  }
  return (total_bits + prop_bits);
}

int main(int argc, char **argv)
{
  FILE* fp_info;
  int fd_vertices, fd_edges;
  char *dummy_ret;
  unsigned long vertex_cnt, alist_entry_cnt, alist_bytes;
  char string_buffer[1024];
  uncompressed_vertex_type *vertex_map;
  unsigned long i, j, keep_compiler_happy;
  unsigned long time_dfs, time_compress, time_remap, time_total;
#ifdef DIMACS
  int fd_rew, fd_co, fd_rew_index;
  unsigned long time_dimacs;
#endif
  CLOCK_START(time_total);
  if(argc < 3) {
    fprintf(stderr, "Usage: %s graph_in graph_out\n", argv[0]);
    exit(-1);
  }
  assert(strlen(argv[1]) < (1023 - 10));
  strcpy(string_buffer, argv[1]);
  strcat(string_buffer, ".info");
  fp_info = fopen(string_buffer, "r");
  if(fp_info == NULL) {
    perror("Unable to open info file");
    exit(-1);
  }
  
  dummy_ret = fgets(string_buffer, 1024, fp_info);
  assert(dummy_ret != NULL);
  dummy_ret = strtok(string_buffer, "=");
  vertex_cnt = atol(strtok(NULL, "="));
  dummy_ret = fgets(string_buffer, 1024, fp_info);
  assert(dummy_ret != NULL);
  dummy_ret = strtok(string_buffer, "=");
  alist_entry_cnt = atol(strtok(NULL, "="));
  dummy_ret = fgets(string_buffer, 1024, fp_info);
  assert(dummy_ret != NULL);
  dummy_ret = strtok(string_buffer, "=");
  alist_bytes = atol(strtok(NULL, "="));
  fclose(fp_info);
  strcpy(string_buffer, argv[1]);
  strcat(string_buffer, ".temp");
  fd_edges = open(string_buffer, O_RDONLY|O_LARGEFILE);
  if(fd_edges == -1) {
    perror("Unable to open input edges file");
    exit(-1);
  }
  strcpy(string_buffer, argv[1]);
  strcat(string_buffer, ".vertices");
  fd_vertices = open(string_buffer, O_RDONLY|O_LARGEFILE);
  if(fd_vertices == -1) {
    perror("Unable to open input vertices file");
    exit(-1);
  }
  vertex_map = 
    (uncompressed_vertex_type *)mmap(0, vertex_cnt*sizeof(uncompressed_vertex_type),
				  PROT_READ, MAP_FILE|MAP_SHARED,
				  fd_vertices, 0);
  if(vertex_map == MAP_FAILED) {
    perror("Unable to map in vertices:");
    exit(-1);
  }
  if(mlock(vertex_map, vertex_cnt * sizeof(uncompressed_vertex_type)) < 0) {
    perror("mlocking vertex map failed:");
  }
  char *alist_map = 
    mmap(0, alist_bytes,
	 PROT_READ, MAP_FILE|MAP_SHARED,
	 fd_edges, 0);
  if(alist_map == MAP_FAILED) {
    perror("Unable to map in edges:");
    exit(-1);
  }
  dfs_metadata_t * dfs_metadata = 
    (dfs_metadata_t*)map_anon_memory(vertex_cnt*sizeof(dfs_metadata_t), "dfs metadata");
  /* Perhaps mmap /dev/null instead ? */
  memset(dfs_metadata, 0, vertex_cnt*sizeof(dfs_metadata_t));
  fprintf(stderr, "Building dfs tree...");
  CLOCK_START(time_dfs);
  unsigned long dfs_tree_roots =
    build_dfs_tree(dfs_metadata, vertex_map, alist_map, vertex_cnt, alist_entry_cnt);
  CLOCK_STOP(time_dfs);
  fprintf(stderr, "Done\nPerforming heuristic enumeration...");
  CLOCK_START(time_remap);
#ifdef REMAP_RANDOM
  identity_remap(dfs_tree_roots, dfs_metadata, vertex_map, vertex_cnt);
  randomise_remap(dfs_metadata, vertex_cnt, 0);
#elif defined(REMAP_HALF_AND_HALF)
  perform_remap(dfs_tree_roots, dfs_metadata, vertex_map, vertex_cnt);
  randomise_remap(dfs_metadata, vertex_cnt, vertex_cnt/2);
#else
  perform_remap(dfs_tree_roots, dfs_metadata, vertex_map, vertex_cnt);
#endif
  CLOCK_STOP(time_remap);
  fprintf(stderr, "Done\nWriting out new graph...\n");
  assert(strlen(argv[2]) < (1023 - 10));
  
  /******** Write out ************/

  unsigned long calist_bits = 0, ivert;
  int fd_info, fd_cvertices, fd_calist;
  vertex_t current_vertex;
  strcpy(string_buffer, argv[2]);
  strcat(string_buffer, ".vertices");
  fd_cvertices = open(string_buffer, O_RDWR|O_CREAT|O_TRUNC|O_LARGEFILE, S_IRWXU);
  if(fd_cvertices == -1) {
    perror("Unable to open vertices file for writing:");
    exit(-1);
  }
  strcpy(string_buffer, argv[2]);
  strcat(string_buffer, ".calist");
  fd_calist = open(string_buffer, O_RDWR|O_CREAT|O_TRUNC|O_LARGEFILE, S_IRWXU);
  if(fd_calist == -1) {
    perror("Unable to open calist file for writing:");
    exit(-1);
  }
  unsigned long periodic_clock;
  unsigned long target_vertices = 1000000;
  CLOCK_START(time_compress);
  CLOCK_START(periodic_clock);
  unsigned char last_byte_written = 0;
  unsigned long calist_bits_to_write;
  for(i=0;i<vertex_cnt;i++) {
    ivert = dfs_metadata[i].stack_next;
    assert(ivert < vertex_cnt);
    current_vertex.offset_edges = calist_bits;
    calist_bits_to_write = write_cal(i, 
				     vertex_map[ivert].degree,
				     &current_vertex, dfs_metadata, 
				     (unsigned long *)
				     &alist_map[vertex_map[ivert].offset_edges],
				     &alist_map[vertex_map[ivert].offset_edge_info],
				     last_byte_written,
				     BIT_INDEX(calist_bits));
    keep_compiler_happy = write(fd_cvertices, &current_vertex,
				sizeof(vertex_t));
    assert(keep_compiler_happy == sizeof(vertex_t));
    if(calist_bits_to_write == 0) {
      continue;
    }
    unsigned long calist_bytes_to_write = 
      ROUND_BYTE(BIT_INDEX(calist_bits) + calist_bits_to_write);
    keep_compiler_happy = write(fd_calist, cal_buffer, calist_bytes_to_write);
    assert(keep_compiler_happy == calist_bytes_to_write);
    calist_bits += calist_bits_to_write;
    if(BIT_INDEX(calist_bits)) {
      keep_compiler_happy = lseek(fd_calist, -1, SEEK_CUR);
      last_byte_written = cal_buffer[calist_bytes_to_write - 1];
    }
    else {
      last_byte_written = 0;
    }
    target_vertices--;
    if(target_vertices == 0) {
      CLOCK_STOP(periodic_clock);
      fprintf(stderr, "Compressed 1000000 vertices time %lu\n", periodic_clock);
      target_vertices = 1000000;     
      CLOCK_START(periodic_clock);
    }
  }
  CLOCK_STOP(time_compress);
#ifdef DIMACS
  strcpy(string_buffer, argv[2]);
  strcat(string_buffer, ".rew");
  fd_rew = open(string_buffer, O_RDWR|O_CREAT|O_TRUNC|O_LARGEFILE, S_IRWXU);
  if(fd_rew == -1) {
    perror("Unable to open rew file for writing:");
    exit(-1);
  }
  strcpy(string_buffer, argv[2]);
  strcat(string_buffer, ".rew_index");
  fd_rew_index = open(string_buffer, O_RDWR|O_CREAT|O_TRUNC|O_LARGEFILE, S_IRWXU);
  if(fd_rew_index == -1) {
    perror("Unable to open rew index file for writing:");
    exit(-1);
  }
  strcpy(string_buffer, argv[2]);
  strcat(string_buffer, ".co");
  fd_co = open(string_buffer, O_RDWR|O_CREAT|O_TRUNC|O_LARGEFILE, S_IRWXU);
  if(fd_co == -1) {
    perror("Unable to open co-ord file for writing:");
    exit(-1);
  }
  target_vertices = 1000000;
  CLOCK_START(time_dimacs);
  CLOCK_START(periodic_clock);
  unsigned long rew_index = 0;
  for(i=0;i<vertex_cnt;i++) {
    ivert = dfs_metadata[i].stack_next;
    keep_compiler_happy = write(fd_co, &vertex_map[ivert].x_coord, sizeof(int));
    assert(keep_compiler_happy == sizeof(int));
    keep_compiler_happy = write(fd_co, &vertex_map[ivert].y_coord, sizeof(int));
    assert(keep_compiler_happy == sizeof(int));
    keep_compiler_happy = 
      write(fd_rew_index, &rew_index, sizeof(unsigned long));
    assert(keep_compiler_happy == sizeof(unsigned long));
    unsigned long *weights = (unsigned long *)
      &alist_map[vertex_map[ivert].offset_edge_weights];
    for(j=0;j<vertex_map[ivert].degree;j++) {
      keep_compiler_happy = 
	write(fd_rew, &weights[j], sizeof(unsigned long));
      assert(keep_compiler_happy == sizeof(unsigned long));
    }
    rew_index += vertex_map[ivert].degree*sizeof(unsigned long);
    target_vertices--;
    if(target_vertices == 0) {
      CLOCK_STOP(periodic_clock);
      fprintf(stderr, "Wrote DIMACS metadata for 1000000 vertices time %lu\n",
	      periodic_clock);
      target_vertices = 1000000;
      CLOCK_START(periodic_clock);
    }
  }
  CLOCK_STOP(time_dimacs);
  close(fd_rew_index);
  close(fd_rew);
  close(fd_co);
#endif
  close(fd_cvertices);
  close(fd_calist);
  strcpy(string_buffer, argv[2]);
  strcat(string_buffer, ".info");
  fd_info = open(string_buffer, O_RDWR|O_CREAT|O_TRUNC, S_IRWXU);
  if(fd_info == -1) {
    perror("Unable to open info file for writing:");
    exit(-1);
  }
  sprintf(string_buffer, "vertices=%lu\n", vertex_cnt);
  keep_compiler_happy = write(fd_info, string_buffer, strlen(string_buffer));
  sprintf(string_buffer, "alist_entries=%lu\n", alist_entry_cnt);
  keep_compiler_happy = write(fd_info, string_buffer, strlen(string_buffer));
  sprintf(string_buffer, "alist_bytes=%lu\n", ROUND_BYTE(calist_bits));
  keep_compiler_happy = write(fd_info, string_buffer, strlen(string_buffer));
  close(fd_info);
  munmap(dfs_metadata, vertex_cnt*sizeof(dfs_metadata_t));
  munmap(vertex_map, vertex_cnt*sizeof(uncompressed_vertex_type));
  munmap(alist_map, alist_bytes);
  close(fd_vertices);
  close(fd_edges);
  CLOCK_STOP(time_total);
  printf("TIME DFS %lu\n", time_dfs);
  printf("TIME REMAP %lu\n", time_remap);
  printf("TIME COMPRESS %lu\n", time_compress);
#ifdef DIMACS
  printf("TIME DIMACS %lu\n", time_dimacs);
#endif
  printf("TIME TOTAL %lu\n", time_total);
  return 0;
}
