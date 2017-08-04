#ifndef _GRAPH_DEFS_
#define _GRAPH_DEFS_

#include "misc_utils.h"


#define ROUND_BYTE(n) (((n) + 7)/8)
#define BYTE_INDEX(n) ((n)/8)
#define BIT_INDEX(n) ((n) & 7)

#define POS_SIGN 0
#define POS_DIR 1

/* Compressed Adjacency List Implementation */
extern char fadvise_test(unsigned long page);
extern void write_pipe(unsigned int thread, unsigned long node, int priority);
typedef struct vertex_st{
  /* Note that offset are BIT offsets    */
  /* This limits CAL files to 2^61 bytes */
  unsigned long offset_edges;
  unsigned long offset_edge_info; /* Bitmap of edge directions and offset signs*/
} vertex_t;

typedef struct uncompressed_vertex_st{
  unsigned long offset_edges;
  unsigned long offset_edge_info; /* Bitmap of edge directions and offset signs*/
  unsigned long degree; /* Number of neighbours */
} uncompressed_vertex_t;

typedef struct uncompressed_dimacs_vertex_st{
  unsigned long offset_edges;
  unsigned long offset_edge_info; /* Bitmap of edge directions and offset signs*/
  unsigned long offset_edge_weights;     /* Location of edge weights */
  unsigned long degree; /* Number of neighbours */
  int x_coord;
  int y_coord;
} uncompressed_dimacs_vertex_t;

typedef struct edge_iterator_st {
  unsigned long index;         /* Index for current edge           */
  unsigned long offset;        /* Offset for next edge             */
  char current_bits_per_edge;  /* Current number of bits per edge  */
  vertex_t * vertex;           /* Originating vertex               */
  unsigned long neighbour;     /* Set by iterator                  */
  char incoming;               /* Set by iterator if in-edge       */
} edge_iterator_t;

typedef struct csr_edge_iterator_st {
  unsigned long offset;        /* Offset for next edge             */
  unsigned long offset_stop;   /* Offset to stop                   */
  unsigned long neighbour;     /* Set by iterator                  */
  char incoming;               /* Set by iterator if in-edge       */
} csr_edge_iterator_t;


typedef struct graph_st {
  vertex_t * vertex_map;     /* Mapped into VM -- do not access directly */
  unsigned char *calist_map; /* Mapped into VM -- do not access directly */
  unsigned long vertex_cnt;
  unsigned long alist_entries;
  unsigned long calist_bytes;
  int fd_calist;
  unsigned long assume_undirected;
} graph_t;

typedef struct csr_st {
  unsigned long *index;     /* Mapped into VM -- do not access directly */
  unsigned long *index_aux; /* Mapped into VM -- do not access directly */
  unsigned char *calist_map; /* Mapped into VM -- do not access directly */
  unsigned char *aux;         /* Mapped into VM -- do not access directly */
  unsigned long vertex_cnt;
  unsigned long alist_entries;
  unsigned long calist_bytes;
  unsigned long aux_bytes;
  int fd_index;
  int fd_index_aux;
  int fd_calist;
  int fd_aux;
  unsigned long assume_undirected;
  unsigned long bits_per_edge;
} csr_t;

/* All errors are fatal ! */
static graph_t* open_vertices(char *graph_name)
{
  FILE* fp_info;
  int fd_vertices;
  int fd_calist;
  char string_buffer[1024], *dummy_ret;
  graph_t *graph = (graph_t *)malloc(sizeof(graph_t));
  if(graph == NULL) {
    fprintf(stderr, "Unable to allocate space for graph\n");
    exit(-1);
  }
  assert(strlen(graph_name) < (1023 - 10));
  strcpy(string_buffer, graph_name);
  strcat(string_buffer, ".info");
  fp_info = fopen(string_buffer, "r");
  if(fp_info == NULL) {
    perror("Unable to open info file");
    exit(-1);
  }
  dummy_ret = fgets(string_buffer, 1024, fp_info);
  assert(dummy_ret != NULL);
  dummy_ret = strtok(string_buffer, "=");
  graph->vertex_cnt = atol(strtok(NULL, "="));
  dummy_ret = fgets(string_buffer, 1024, fp_info);
  assert(dummy_ret != NULL);
  dummy_ret = strtok(string_buffer, "=");
  graph->alist_entries = atol(strtok(NULL, "="));
  dummy_ret = fgets(string_buffer, 1024, fp_info);
  assert(dummy_ret != NULL);
  dummy_ret = strtok(string_buffer, "=");
  graph->calist_bytes = atol(strtok(NULL, "="));
  fclose(fp_info);
  strcpy(string_buffer, graph_name);
  strcat(string_buffer, ".vertices");
  fd_vertices = open(string_buffer, O_RDONLY|O_LARGEFILE);
  if(fd_vertices == -1) {
    perror("Unable to open input vertices file");
    exit(-1);
  }
  graph->vertex_map =
    (vertex_t *)mmap(0, graph->vertex_cnt*sizeof(vertex_t),
		     PROT_READ, MAP_FILE|MAP_SHARED,
		     fd_vertices, 0);
  if(graph->vertex_map == MAP_FAILED) {
    perror("Unable to map in vertices:");
    exit(-1);
  }
  close(fd_vertices);
  strcpy(string_buffer, graph_name);
  strcat(string_buffer, ".calist");
  fd_calist = open(string_buffer, O_RDONLY|O_LARGEFILE);
  if(fd_calist == -1) {
    perror("Unable to open input edges file");
    exit(-1);
  }
#ifndef __APPLE__
  int adv_ret =
    posix_fadvise(fd_calist, 0, graph->calist_bytes, POSIX_FADV_RANDOM);
  if(adv_ret) {
    perror("Unable to disable os prefetching on calist file");
    exit(-1);
  }
#endif
  graph->fd_calist = fd_calist;
  /* Vertex memory is mlocked to achieve
   * semi-external memory IO performance
   */
  if(mlock(graph->vertex_map,
	   graph->vertex_cnt*sizeof(vertex_t)) < 0) {
    perror("mlock failed on vertex file:");
    exit(-1);
  }
  register_mlocked_memory(graph->vertex_cnt*sizeof(vertex_t));
  graph->assume_undirected = (getenv("ASSUME_UNDIRECTED") != NULL);
  return graph;
}

/* All errors are fatal ! */



static csr_t* open_csr_vertices(char *graph_name)
{
  FILE* fp_info;
  int fd_vertices;
  int fd_calist;
  char string_buffer[1024], *dummy_ret;
  csr_t *graph = (csr_t *)malloc(sizeof(csr_t));
  if(graph == NULL) {
    fprintf(stderr, "Unable to allocate space for graph\n");
    exit(-1);
  }
  assert(strlen(graph_name) < (1023 - 10));
  strcpy(string_buffer, graph_name);
  strcat(string_buffer, ".info");
  fp_info = fopen(string_buffer, "r");
  if(fp_info == NULL) {
    perror("Unable to open info file");
    exit(-1);
  }
  dummy_ret = fgets(string_buffer, 1024, fp_info);
  assert(dummy_ret != NULL);
  dummy_ret = strtok(string_buffer, "=");
  graph->vertex_cnt = atol(strtok(NULL, "="));
  dummy_ret = fgets(string_buffer, 1024, fp_info);
  assert(dummy_ret != NULL);
  dummy_ret = strtok(string_buffer, "=");
  graph->alist_entries = atol(strtok(NULL, "="));
  dummy_ret = fgets(string_buffer, 1024, fp_info);
  assert(dummy_ret != NULL);
  dummy_ret = strtok(string_buffer, "=");
  graph->calist_bytes = atol(strtok(NULL, "="));
  fclose(fp_info);
  strcpy(string_buffer, graph_name);
  strcat(string_buffer, ".vertices");
  fd_vertices = open(string_buffer, O_RDONLY|O_LARGEFILE);
  if(fd_vertices == -1) {
    perror("Unable to open input vertices file");
    exit(-1);
  }
  graph->index =
    (unsigned long *)mmap(0, graph->vertex_cnt*sizeof(unsigned long),
			  PROT_READ, MAP_FILE|MAP_SHARED,
			  fd_vertices, 0);
  if(graph->index == MAP_FAILED) {
    perror("Unable to map in vertices:");
    exit(-1);
  }
  close(fd_vertices);
  strcpy(string_buffer, graph_name);
  strcat(string_buffer, ".calist");
  fd_calist = open(string_buffer, O_RDONLY|O_LARGEFILE);
  if(fd_calist == -1) {
    perror("Unable to open input edges file");
    exit(-1);
  }
#ifndef __APPLE__
  int adv_ret =
    posix_fadvise(fd_calist, 0, graph->calist_bytes, POSIX_FADV_RANDOM);
  if(adv_ret) {
    perror("Unable to disable os prefetching on calist file");
    exit(-1);
  }
#endif
  graph->fd_calist = fd_calist;
  /* Vertex memory is mlocked to achieve
   * semi-external memory IO performance
   */
  if(mlock(graph->index,
	   graph->vertex_cnt*sizeof(unsigned long)) < 0) {
    perror("mlock failed on vertex file:");
    exit(-1);
  }
  register_mlocked_memory(graph->vertex_cnt*sizeof(unsigned long));
  graph->assume_undirected = (getenv("ASSUME_UNDIRECTED") != NULL);
  unsigned long max_vertex = graph->vertex_cnt - 1;
  graph->bits_per_edge = 0;
  do {
    graph->bits_per_edge++;
  } while(max_vertex = (max_vertex >> 1));
  graph->bits_per_edge++;
  graph->index_aux = NULL;
  graph->aux = NULL;
  return graph;
}

static csr_t* open_csr(char *graph_name)
{
  csr_t* graph = open_csr_vertices(graph_name);
  graph->calist_map =
    mmap(0, graph->calist_bytes,
	 PROT_READ, MAP_FILE|MAP_SHARED,
	 graph->fd_calist, 0);
  if(graph->calist_map == MAP_FAILED) {
    perror("Unable to map in edges:");
    exit(-1);
  }
  return graph;
}

static csr_t* open_csr_aux(char *graph_name)
{
  unsigned char string_buffer[1024];
  unsigned long keep_compiler_happy;

  csr_t* graph = open_csr(graph_name);

  strcpy(string_buffer, graph_name);
  strcat(string_buffer, ".rew_index");
  int fd_rew_index = open(string_buffer, O_RDONLY|O_LARGEFILE, S_IRWXU);
  if(fd_rew_index == -1) {
    perror("Unable to open rew index file:");
    exit(-1);
  }
  graph->index_aux = (unsigned long *)
    mmap(0,  (graph->vertex_cnt)*sizeof(unsigned long),
	 PROT_READ, MAP_SHARED|MAP_FILE, fd_rew_index, 0);
  if(graph->index_aux == MAP_FAILED) {
    perror("Unable to map rew index:");
    exit(-1);
  }
  if(mlock(graph->index_aux, (graph->vertex_cnt)*sizeof(unsigned long)) < 0) {
    perror("mlock failed on rew index:");
    exit(-1);
  }
  register_mlocked_memory(graph->vertex_cnt*sizeof(unsigned long));


  strcpy(string_buffer, graph_name);
  strcat(string_buffer, ".rew");
  int fd_rew = open(string_buffer, O_RDONLY|O_LARGEFILE, S_IRWXU);
  if(fd_rew == -1) {
    perror("Unable to open rew file:");
    exit(-1);
  }
  unsigned long rew_size = lseek(fd_rew, 0, SEEK_END);
  keep_compiler_happy = lseek(fd_rew, 0, SEEK_SET);
  assert(keep_compiler_happy == 0);
  graph->aux = (unsigned char *)
    mmap(0,  rew_size,
	 PROT_READ, MAP_SHARED|MAP_FILE, fd_rew, 0);
  if(graph->aux == MAP_FAILED) {
    perror("Unable to map rew:");
    exit(-1);
  }
  graph->fd_aux = fd_rew;
  graph->aux_bytes = rew_size;
  return graph;
}

void open_cal(graph_t *graph)
{
  graph->calist_map =
    mmap(0, graph->calist_bytes,
	 PROT_READ, MAP_FILE|MAP_SHARED,
	 graph->fd_calist, 0);
  if(graph->calist_map == MAP_FAILED) {
    perror("Unable to map in edges:");
    exit(-1);
  }
}



static graph_t *open_graph(char *graph_name)
{
  graph_t* graph = open_vertices(graph_name);
  open_cal(graph);
  return graph;
}

static void close_graph(graph_t *graph)
{
  munmap(graph->vertex_map, graph->vertex_cnt*sizeof(vertex_t));
  munmap(graph->calist_map, graph->calist_bytes);
  close(graph->fd_calist);
  // TBD: fix memleak on graph_t object.. who should free it ?
}


static void close_csr(csr_t *graph)
{
  munmap(graph->index, graph->vertex_cnt*sizeof(unsigned long));
  munmap(graph->calist_map, graph->calist_bytes);
  close(graph->fd_index);
  close(graph->fd_calist);
  if(graph->aux != NULL){
    munmap(graph->index_aux, graph->vertex_cnt*sizeof(unsigned long));
    munmap(graph->aux, graph->aux_bytes);
    close(graph->fd_index_aux);
    close(graph->fd_aux);
  }
  // TBD: fix memleak on graph_t object.. who should free it ?
}


static  void init_edge_iterator(graph_t *graph,
				unsigned long vertex,
				edge_iterator_t *iter)
{
  iter->index = -1;
  iter->vertex = &graph->vertex_map[vertex];
  iter->offset =iter->vertex->offset_edges;
  iter->current_bits_per_edge = 1;
}

static void csr_init_edge_iterator(csr_t *graph,
				   unsigned long vertex,
				   csr_edge_iterator_t *iter)
{


  iter->offset = graph->index[vertex];
  if(vertex < (graph->vertex_cnt - 1)) {
    iter->offset_stop = graph->index[vertex + 1];
  }
  else {
    iter->offset_stop = graph->alist_entries*graph->bits_per_edge;
  }
}


unsigned long vertex_index(vertex_t *vertex, graph_t *graph)
{
  return (vertex - graph->vertex_map);
}

/* Returns 0 if success else -1. Only pass in an inited iterator */
/* Loads fields of the iterator for subsequent access */
static int iter_step(graph_t *graph, edge_iterator_t *iter)
{
  unsigned long prop_offset;
  unsigned long prop_index;
  iter->index++;
  if(iter->offset == iter->vertex->offset_edge_info) {
    return -1; /* End of CAL */
  }
  while((graph->calist_map[BYTE_INDEX(iter->offset)] &
	 (1<<BIT_INDEX(iter->offset))) == 0) {
    iter->offset++;
    iter->current_bits_per_edge++;
  }
  assert(iter->offset < iter->vertex->offset_edge_info);
  assert(iter->current_bits_per_edge < 65);
  /* Works only for little endian ! */
  __uint128_t tmp;
  unsigned long start_byte = BYTE_INDEX(iter->offset);
  unsigned long stop_byte = BYTE_INDEX(iter->offset +
				       iter->current_bits_per_edge - 1);
  memcpy(&tmp, &graph->calist_map[start_byte],
	 (stop_byte - start_byte) + 1);
  tmp >>= BIT_INDEX(iter->offset);
  iter->neighbour = (unsigned long)tmp;
  iter->neighbour &= ((1UL << iter->current_bits_per_edge) - 1);
  /* Twizzle back last bit */
  assert((iter->neighbour & 1) != 0);
  iter->neighbour >>= 1;
  iter->neighbour |= (1UL << (iter->current_bits_per_edge - 1));
  iter->offset += iter->current_bits_per_edge;
  assert(iter->offset <= iter->vertex->offset_edge_info);
  prop_index = iter->vertex->offset_edge_info + 2*iter->index + POS_SIGN;
  prop_offset = BYTE_INDEX(prop_index);
  if(graph->calist_map[prop_offset] & (1 << BIT_INDEX(prop_index))) {
    iter->neighbour = vertex_index(iter->vertex, graph) - iter->neighbour;
  }
  else {
    iter->neighbour = iter->neighbour + vertex_index(iter->vertex, graph);
  }
  prop_index = iter->vertex->offset_edge_info + 2*iter->index + POS_DIR;
  prop_offset = BYTE_INDEX(prop_index);
  if(!(graph->assume_undirected)) {
    if(graph->calist_map[prop_offset] & (1 << BIT_INDEX(prop_index))) {
      iter->incoming = 1;
    }
    else {
      iter->incoming = 0;
    }
  }
  else {
    iter->incoming = 0;
  }
  assert(iter->neighbour < graph->vertex_cnt);
  return 0;
}

/* Returns 0 if success else -1. Only pass in an inited iterator */
/* Loads fields of the iterator for subsequent access */
static int csr_iter_step(csr_t *graph, csr_edge_iterator_t *iter)
{
  assert(iter->offset <= iter->offset_stop);
  if(iter->offset == iter->offset_stop) {
    return -1;
  }
  /* Works only for little endian ! */
  __uint128_t tmp;
  unsigned long start_byte = BYTE_INDEX(iter->offset);
  unsigned long stop_byte = BYTE_INDEX(iter->offset +
				       graph->bits_per_edge - 1);
  memcpy(&tmp, &graph->calist_map[start_byte],
	 (stop_byte - start_byte) + 1);
  tmp >>= BIT_INDEX(iter->offset);
  tmp &= ((((__uint128_t)1) << graph->bits_per_edge) - 1);
  iter->incoming = ((tmp & 1) != 0);
  tmp >>= 1;
  iter->neighbour = (unsigned long)tmp;
  iter->offset += graph->bits_per_edge;
  if(graph->assume_undirected) {
    iter->incoming = 0;
  }
  assert(iter->neighbour < graph->vertex_cnt);
  return 0;
}

/* Various container utilities */

// DFS stack, assumes item has stack_next member
#define DFS_PUSH(_top, _item, _metadata)	\
  do {						\
    (_metadata)[(_item)].stack_next = (_top);	\
    (_top) = _item;				\
  } while(0)

#define DFS_POP(_top, _metadata)		\
  do {						\
    (_top) = (_metadata)[(_top)].stack_next ;	\
  } while(0)

// CC stack, assumes item has cc_stack_next member
#define CC_PUSH(_top, _item, _metadata)	\
  do {						\
    (_metadata)[(_item)].cc_stack_next = (_top);	\
    (_top) = _item;				\
  } while(0)

#define CC_POP(_top, _metadata)		\
  do {                      \
    unsigned long _tmp = (_metadata)[(_top)].cc_stack_next; \
    (_metadata)[(_top)].cc_stack_next = 0; \
    (_top) = _tmp;	\
  } while(0)

#define LIST1_PUSH(_last, _item, _metadata) \
  do {                                       \
    (_metadata)[(_item)].list1_next = _last; \
    (_last) = _item;                          \
  }while(0)

#define LIST1_NEXT(_iter, _metadata)          \
  do {                                       \
     (_iter) = (_metadata)[(_iter)].list1_next;\
  }while(0)

#define LIST2_PUSH(_last, _item, _metadata) \
  do {                                       \
    (_metadata)[(_item)].list2_next = _last; \
    (_last) = _item;                          \
  }while(0)

#define LIST2_NEXT(_iter, _metadata)          \
  do {                                       \
     (_iter) = (_metadata)[(_iter)].list2_next;\
  }while(0)
// BFS queue, assumes item has queue_next member
// Note that the BFS queue needs to be thread-safe with respect to the
// prefetcher. This is achieved by ensuring that queue pop
// does not change the next field
// and queue push changes an aligned volatile field

#define BFS_PUSH(_qhead, _qtail, _item, _metadata)	\
  do {							\
    _metadata[(_item)].queue_next = ULONG_MAX;		\
    if((_qhead) == ULONG_MAX) {				\
      (_qhead) = (_qtail) = (_item);			\
    }							\
    else {						\
      _metadata[(_qtail)].queue_next = (_item);		\
      (_qtail) = (_item);				\
    }							\
  } while(0)

#define BFS_STITCH(_qhead, _qtail, _sqhd, _sqtl, _metadata)      \
  do {                                                  \
    if ((_sqtl) == ULONG_MAX) break;			\
    _metadata[(_sqtl)].queue_next = ULONG_MAX;          \
    if((_qhead) == ULONG_MAX) {                         \
      (_qhead) = (_sqhd); (_qtail) = (_sqtl);                    \
    }                                                   \
    else {                                              \
      _metadata[(_qtail)].queue_next = (_sqhd);         \
      (_qtail) = (_sqtl);                               \
    }                                                   \
  } while(0)


#define BFS_POP(_qhead, _qtail, _metadata) ({		\
      unsigned long _item = (_qhead);			\
      if((_qhead) == (_qtail)) {			\
	(_qhead) = (_qtail) = ULONG_MAX;		\
      }							\
      else {						\
	(_qhead) = _metadata[(_qhead)].queue_next;	\
      }							\
      _item; })



#define BFS_LIST_APPEND(_tgt_head, _tgt_tail, _src_head, _src_tail, _metadata) \
  do {									\
    if((_tgt_head) == ULONG_MAX) {					\
      _tgt_head = _src_head;						\
      _tgt_tail = _src_tail;						\
    }									\
    else {								\
      _metadata[_tgt_tail].queue_next = _src_head;			\
      _tgt_tail = _src_tail;						\
    }}while(0)

/*  A simplistic binary heap implementation */

#ifndef HEAP_KEY_TYPE
#define HEAP_KEY_TYPE double
#endif

typedef struct heap_entry_st {
  HEAP_KEY_TYPE key;
  /* pointer kept updated with index on the heap */
  unsigned long *indexp __attribute__((aligned(8)));
} heap_entry_t;
/* Note: for the prefetcher to work indexp must be atomically updated
 * on the x86 this is guaranteed if it is aligned on an 8 byte boundary,
 * which gcc should do !
 */

typedef struct heap_st {
  heap_entry_t * heap_entries;
  unsigned long heap_size;
  volatile unsigned long heap_bottom;
} heap_t;

static heap_t *allocate_heap(unsigned long size)
{
  heap_t * heap = (heap_t *)malloc(sizeof(heap_t));
  assert(heap != NULL);
  heap->heap_size = size;
  heap->heap_bottom = ULONG_MAX;
  heap->heap_entries = (heap_entry_t*)
    map_anon_memory(size*sizeof(heap_entry_t), "heap");
  memset(heap->heap_entries, 0, size*sizeof(heap_entry_t));
  return heap;
}

#define HEAP_SWAP_HELPER(_l, _r, _tmp)		\
  do {						\
    _tmp = _l;					\
    _l = _r;					\
    _r = _tmp;					\
  } while(0)

#define SWAP_HEAP_ENTRIES(_heap, _i, _j)			\
  do{								\
    unsigned long *_tmp_indexp;					\
    HEAP_KEY_TYPE _tmp_key;					\
    *((_heap)->heap_entries[_i].indexp) = _j;			\
    *((_heap)->heap_entries[_j].indexp) = _i;			\
    HEAP_SWAP_HELPER((_heap)->heap_entries[_i].key,		\
		     (_heap)->heap_entries[_j].key,		\
		     _tmp_key);					\
    HEAP_SWAP_HELPER((_heap)->heap_entries[_i].indexp,		\
		     (_heap)->heap_entries[_j].indexp,		\
		     _tmp_indexp);				\
  } while(0)

static void bubble_up(heap_t *heap, unsigned long entry) {
  while(entry != 0) {
    unsigned long parent = (entry - 1)/2;
    if(heap->heap_entries[parent].key >
       heap->heap_entries[entry].key) { /* bubble up */
      SWAP_HEAP_ENTRIES(heap, parent, entry);
      entry = parent;
    }
    else {
      break;
    }
  }
}

static void heap_add(heap_t *heap,
		     HEAP_KEY_TYPE key,
		     unsigned long* indexp)
{
  if(heap->heap_bottom == ULONG_MAX) {
    heap->heap_bottom = 0;
  }
  else {
    heap->heap_bottom++;
  }
  heap->heap_entries[heap->heap_bottom].key = key;
  heap->heap_entries[heap->heap_bottom].indexp = indexp;
  *indexp = heap->heap_bottom;
  bubble_up(heap, heap->heap_bottom);
}
static void heap_reduce_key(heap_t *heap,
			    unsigned long entry,
			    HEAP_KEY_TYPE key)
{
  heap->heap_entries[entry].key = key;
  bubble_up(heap, entry);
}

static HEAP_KEY_TYPE heap_get_key(heap_t *heap,
				  unsigned long heap_index)
{
  assert(heap_index <= heap->heap_bottom);
  return heap->heap_entries[heap_index].key;
}

static int heap_is_empty(heap_t * heap)
{
  return (heap->heap_bottom == ULONG_MAX);
}

/* returns min-key */
static HEAP_KEY_TYPE heap_get_min_key(heap_t *heap)
{
  return heap->heap_entries[0].key;
}

/* returns min-index */
static unsigned long* heap_get_min_index(heap_t *heap)
{
  return heap->heap_entries[0].indexp;
}

/* removes min-key */
static unsigned long heap_remove_min(heap_t *heap)
{
  unsigned long return_value = 0;
  unsigned long bubble;
  if(heap->heap_bottom == 0) {
    heap->heap_bottom = ULONG_MAX;
    return;
  }
  else {
    SWAP_HEAP_ENTRIES(heap, 0, heap->heap_bottom);
    heap->heap_bottom--;
  }
  bubble = 0;
  while(1) {
    unsigned long child_l = 2*bubble + 1;
    unsigned long child_r = 2*bubble + 2;
    unsigned long swap_child;
    if(child_l > heap->heap_bottom) {
      break;
    }
    swap_child = child_l;
    if(child_r <= heap->heap_bottom &&
       (heap->heap_entries[child_r].key <
	heap->heap_entries[child_l].key)) {
      swap_child = child_r;
    }
    if(heap->heap_entries[swap_child].key <
       heap->heap_entries[bubble].key) { /* bubble down */
      SWAP_HEAP_ENTRIES(heap, bubble, swap_child);
#ifdef PREFETCHER2
        if(bubble <32){
            return_value = bubble;
        }
#endif
      bubble = swap_child;
    }
    else {
      break;
    }
  }
  return return_value;
}

/* Peek into a position on the heap */
static unsigned long *heap_peek(heap_t *heap, unsigned long heap_pos)
{
  return heap->heap_entries[heap_pos].indexp;
}

/* note -- needs a comparator */

static void destroy_heap(heap_t *heap)
{
  munmap(heap->heap_entries, sizeof(heap_entry_t)*heap->heap_size);
  free(heap);
}

#endif
