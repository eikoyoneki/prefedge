#include <limits.h>
#include <cuda_runtime.h>
#include <assert.h>
#include <stdio.h>
#define BYTE_INDEX(n) ((n)/8)
#define BIT_INDEX(n) ((n) & 7)
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

typedef struct csr_edge_iterator_st {
  unsigned long offset;        /* Offset for next edge             */
  unsigned long offset_stop;   /* Offset to stop                   */
  unsigned long neighbour;     /* Set by iterator                  */
  char incoming;               /* Set by iterator if in-edge       */
} csr_edge_iterator_t;


typedef struct bfs_metadata_st {
  int touched;
} bfs_metadata_t;


extern bfs_metadata_t *metadata;
__device__ unsigned long d_visited;
__constant__ int bits_per_edge;
__constant__ unsigned long vertex_cnt;
__constant__ unsigned long alist_bits;

__device__ static int d_csr_iter_step(unsigned char *calist_map, csr_edge_iterator_t *iter)
{
  if(iter->offset == iter->offset_stop) {
    return -1;
  }
  /* Works only for little endian ! */
  unsigned long tmp;
  unsigned long start_byte = BYTE_INDEX(iter->offset);
  unsigned long stop_byte = BYTE_INDEX(iter->offset + bits_per_edge - 1);
  memcpy(&tmp, &calist_map[start_byte], (stop_byte - start_byte) + 1);
  tmp >>= BIT_INDEX(iter->offset);
  tmp &= ((((unsigned long)1) << bits_per_edge) - 1);
  iter->incoming = ((tmp & 1) != 0);
  tmp >>= 1;
  iter->neighbour = (unsigned long)tmp;
  iter->offset += bits_per_edge;
  return 0;
}

__device__ static void d_csr_init_edge_iterator(unsigned long *index,
    unsigned long vertex,
    csr_edge_iterator_t *iter)
{


  iter->offset = index[vertex];
  if(vertex < (vertex_cnt - 1)) {
    iter->offset_stop = index[vertex + 1];
  }
  else {
    iter->offset_stop = alist_bits;
    //graph->alist_entries*graph->bits_per_edge;
  }
}


__global__ void kBFS(int* metadata, unsigned long* index, unsigned char* calist_map, int level) {

  unsigned long current_vertex = blockIdx.x*blockDim.x + threadIdx.x;
  int WARP_ID = current_vertex ;
  if ((WARP_ID < vertex_cnt) && (metadata[WARP_ID] == level)){
    csr_edge_iterator_t iter;
    d_csr_init_edge_iterator(index, WARP_ID, &iter);
    while (d_csr_iter_step(calist_map, &iter) == 0) {
//      if (!iter.incoming) {
	unsigned long target = iter.neighbour;
	/*	if (metadata[target]==0) {
		metadata[target] = level+1;
		d_visited = level+1;
		}
	 */	
	if (atomicCAS(&metadata[target], 0, level+1) == 0) {
	  atomicAdd((unsigned long long*)&d_visited,1ULL);
	}
  //    }
    }
  }
}


extern "C" unsigned long bfs(csr_t *graph, unsigned long start_node) {
  unsigned long* calist_index, *calist_index_cp;
  unsigned char* calist_map, *calist_map_cp;
  char *env_var;
  int TPB = 32;
  env_var = getenv("OMP_NUM_THREADS");
  if(env_var != NULL) {
    TPB = atol(env_var);
  }

  int gridBlocks = ((graph->vertex_cnt) / TPB) + 1;
  cudaSetDeviceFlags(cudaDeviceMapHost);
  cudaHostAlloc(&calist_index_cp, graph->vertex_cnt * sizeof(unsigned long), cudaHostAllocMapped);
  memcpy(calist_index_cp, graph->index, graph->vertex_cnt * sizeof(unsigned long));
  //cudaHostRegister(graph->index, graph->vertex_cnt * sizeof(unsigned long), cudaHostRegisterMapped);
  cudaHostGetDevicePointer(&calist_index, calist_index_cp, 0);
  //cudaHostRegister(graph->calist_map, graph->calist_bytes, cudaHostRegisterMapped);
  cudaHostAlloc(&calist_map_cp, graph->calist_bytes, cudaHostAllocMapped);
  memcpy(calist_map_cp, graph->calist_map, graph->calist_bytes);
  cudaHostGetDevicePointer(&calist_map, calist_map_cp, 0);
  //cudaHostAlloc(&metadata, graph->vertex_cnt * sizeof(bfs_metadata_t), cudaHostAllocMapped);

  int* d_metadata;
  cudaMalloc(&d_metadata, graph->vertex_cnt * sizeof(bfs_metadata_t));
  cudaMemset(d_metadata, 0, graph->vertex_cnt * sizeof(bfs_metadata_t));
  //cudaHostGetDevicePointer(&d_metadata, metadata, 0);
  //cudaMalloc((void**) &d_visited, sizeof(unsigned long));
  //cudaMalloc((void**) &bits_per_edge, sizeof(int));
  //cudaMalloc((void**) &vertex_cnt, sizeof(unsigned long));
  //cudaMalloc((void**) &alist_bits, sizeof(unsigned long));
  cudaMemcpyToSymbol(bits_per_edge, &(graph->bits_per_edge), sizeof(int));
  cudaMemcpyToSymbol(vertex_cnt, &(graph->vertex_cnt), sizeof(unsigned long));
  unsigned long halist_bits = graph->bits_per_edge * graph->alist_entries;
  cudaMemcpyToSymbol(alist_bits, &halist_bits, sizeof(unsigned long));
  cudaDeviceSynchronize();
  unsigned long visited = 1;
  int level = 0;
  //metadata[start_node].touched = 1;
  cudaMemcpy(&d_metadata[start_node], &visited, sizeof(bfs_metadata_t),cudaMemcpyHostToDevice);
  while (1) {
    level++;
    printf("> Running iteration %d ...\n", level);
    cudaMemcpyToSymbol(d_visited, &visited, sizeof(visited));

    assert(cudaGetLastError()==0);
    kBFS<<<gridBlocks,TPB>>>((int*) d_metadata, calist_index, calist_map, level);
    assert(cudaGetLastError()==0);
    cudaDeviceSynchronize();
    unsigned long old_visited = visited;
    cudaMemcpyFromSymbol(&visited, d_visited, sizeof(visited));
    if (old_visited == visited) break;
    printf("> Visited %ld ...\n", visited);

  }

  return 1;
}

