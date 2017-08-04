TARGETS := static_optimisation static_optimisation_random static_optimisation_nodfs\
static_measures \
seq_read_benchmark rand_read_benchmark libprefetcher.so\
seq_read_prefetch_benchmark rand_read_prefetch_benchmark\
graph500_converter dimacs_converter\
ceesor2csr static_measures_csr\
bfs_csr sssp_csr pagerank_csr graph500_to_csr gen_random_edge_weights_csr\
bfs_csr_prefetch sssp_csr_prefetch_aux pagerank_csr_prefetch bfs_csr_prefetch bfs_csr_time_prefetch bfs_csr_mt bfs_csr_bsp\
libprefetcher2.so cores_csr cores_csr_prefetch ssp2 prim_csr prim_csr_prefetch_aux scc_csr scc_csr_prefetch\
sim_csr sim_csr_prefetch a-star_csr a-star_csr_prefetch_aux deg_dist_csr

#gcc prefetcher2.c `pkg-config --cflags --libs glib-2.0`  -lpthread -DPREFETCHER -g -c -fpic -o prefetcher2.o
#gcc prefetcher2.o `pkg-config --cflags --libs glib-2.0`  -lpthread -DPREFETCHER -shared -lc -fpic -o libprefetcher2.so
#gcc sssp_csr2.c `pkg-config --cflags --libs glib-2.0`  -g -lpthread -DPREFETCHER -o ssp2 libprefetcher2.so


#These no longer work --  mostly due to file format changes !
OBSOLETE_TARGETS := layout_bfs gen_random_edge_weights_bfs\
graph500_converter_directed graph500_converter_undirected

DEPS := graph_defs.h misc_utils.h prefetcher.h
LDLIBS := libprefetcher.so  

all: $(LDLIBS) $(TARGETS)

LDLIBS += -lpthread

XLDLIBS = `pkg-config --cflags --libs glib-2.0`

CC := gcc
CFLAGS := -g -O0
#CFLAGS := -pg -Werror

#CFLAGS += -DNDEBUG

deg_dist_csr: deg_dist_csr.c $(DEPS)
	$(CC) $(CFLAGS) $< -o $@ $(LDLIBS)
graphgen_converter_directed: graphgen_converter.c $(DEPS)
	$(CC) $(CFLAGS) $< -o $@ $(LDLIBS)
graphgen_converter_undirected: graphgen_converter.c $(DEPS)
	$(CC) $(CFLAGS) -DMAKE_UNDIRECTED $< -o $@ $(LDLIBS)
graph500_converter: graph500_converter.c $(DEPS)
graph500_to_csr: graph500_to_csr.c $(DEPS)
dimacs_converter: dimacs_converter.c $(DEPS)
static_optimisation: static_optimisation.c $(DEPS)

bfs: bfs.c $(DEPS)
	$(CC) $(CFLAGS) $<  -o $@ $(LDLIBS)
bfs_csr_cuda.x: bfs_csr_cuda_main.c bfs_csr_cuda.o $(DEPS)
	$(CC) $(CFLAGS) -g -L/usr/local/cuda/lib64 $< bfs_csr_cuda.o -lcudart -o $@
bfs_csr_cuda: bfs_csr_cuda.cu bfs_csr_cuda_main.c
	/usr/local/cuda/bin/nvcc -Xcompiler -rdynamic -lineinfo -I/usr/local/cuda/include -I. -arch sm_21 $+ -o $@
bfs_csr: bfs_csr.c $(DEPS)
	$(CC) $(CFLAGS) $<  -o $@ $(LDLIBS)
bfs_csr_mt: bfs_csr_mt.c $(DEPS)
	$(CC) $(CFLAGS) -fopenmp $<  -o $@ $(LDLIBS)
bfs_csr_bsp: bfs_csr_bsp.c $(DEPS)
	$(CC) $(CFLAGS) -fopenmp $<  -o $@ $(LDLIBS)
bfs_prefetch: bfs.c $(DEPS) 
	$(CC) $(CFLAGS) -DPREFETCHER $<  -o $@ $(LDLIBS)
bfs_csr_prefetch: bfs_csr.c $(DEPS) 
	$(CC) $(CFLAGS) -DPREFETCHER $<  -o $@ $(LDLIBS)
bfs_csr_time_prefetch: bfs_csr_time.c $(DEPS)
	$(CC) $(CFLAGS) -DPREFETCHER $<  -o $@ $(LDLIBS) -lrt
	
cores_csr: cores.c $(DEPS)
	$(CC) $(CFLAGS) $<  -o $@ $(LDLIBS)
cores_csr_prefetch: cores.c $(DEPS)
	$(CC) $(CFLAGS) -DPREFETCHER $<  -o $@ $(LDLIBS)

dfs: dfs.c $(DEPS)
layout_bfs: layout_bfs.c $(DEPS)
gen_random_edge_weights: gen_random_edge_weights.c $(DEPS)
gen_random_edge_weights_csr: gen_random_edge_weights_csr.c $(DEPS)

gen_random_edge_weights_bfs: gen_random_edge_weights.c $(DEPS)
	$(CC) $(CFLAGS) -DBFS_LAYOUT_PROP $< -o $@ $(LDLIBS)

static_measures: static_measures.c $(DEPS)
	$(CC) $(CFLAGS) $< -o $@ -lm $(LDLIBS)
static_measures_csr: static_measures_csr.c $(DEPS)
	$(CC) $(CFLAGS) $< -o $@ -lm $(LDLIBS)
static_optimisation_random: static_optimisation.c $(DEPS)
	$(CC) $(CFLAGS) -DREMAP_RANDOM $< -o $@ $(LDLIBS)
static_optimisation_nodfs: static_optimisation.c $(DEPS)
	$(CC) $(CFLAGS) -DREMAP_IDENT $< -o $@ $(LDLIBS)
static_optimisation_hah: static_optimisation.c $(DEPS)
	$(CC) $(CFLAGS) -DREMAP_HALF_AND_HALF $< -o $@ $(LDLIBS)
static_optimisation_dimacs: static_optimisation.c $(DEPS)
	$(CC) $(CFLAGS) -DDIMACS $< -o $@ $(LDLIBS) -lm
static_optimisation_dimacs_random: static_optimisation.c $(DEPS)
	$(CC) $(CFLAGS) -DDIMACS -DREMAP_RANDOM $< -o $@ $(LDLIBS) -lm
static_optimisation_dimacs_hah: static_optimisation.c $(DEPS)
	$(CC) $(CFLAGS) -DDIMACS -DREMAP_HALF_AND_HALF $< -o $@ $(LDLIBS) -lm

dotify_cal: dotify_cal.c $(DEPS)
sssp: sssp.c $(DEPS)
sssp_csr: sssp_csr.c $(DEPS)

ssp2: sssp_csr2.c $(DEPS) 
	$(CC) $(XLDLIBS) $(CFLAGS) -DPREFETCHER -DPREFETCHER2 -DNDEBUG  $<  -o $@ libprefetcher2.so
	
ssp3: sssp_csr3.c $(DEPS) 
	$(CC) $(XLDLIBS) $(CFLAGS) -DPREFETCHER $<  -o $@ libprefetcher3.so

sssp_prefetch: sssp.c $(DEPS) 
	$(CC) $(CFLAGS) -DPREFETCHER $<  -o $@ $(LDLIBS)
sssp_prefetch_aux: sssp.c $(DEPS) 
	$(CC) $(CFLAGS) -DPREFETCHER -DAUX_PREFETCH $<  -o $@ $(LDLIBS)
sssp_csr_prefetch_aux: sssp_csr.c $(DEPS) 
	$(CC) $(CFLAGS) -DPREFETCHER -DNDEBUG -DAUX_PREFETCH $<  -o $@ $(LDLIBS)
sssp-trunc: sssp.c $(DEPS)
	$(CC) $(CFLAGS) -DTRUNC_ALG $<  -o $@ $(LDLIBS)
sssp-trunc_prefetch: sssp.c $(DEPS) 
	$(CC) $(CFLAGS) -DTRUNC_ALG -DPREFETCHER $<  -o $@ $(LDLIBS)
sssp-trunc_prefetch_aux: sssp.c $(DEPS) 
	$(CC) $(CFLAGS) -DTRUNC_ALG -DPREFETCHER -DAUX_PREFETCH $<  -o $@ $(LDLIBS)

mst: mst.c $(DEPS)

mst_prefetch: mst.c $(DEPS) 
	$(CC) $(CFLAGS) -DPREFETCHER $<  -o $@ $(LDLIBS)
mst_prefetch_aux: mst.c $(DEPS) 
	$(CC) $(CFLAGS) -DPREFETCHER -DAUX_PREFETCH $<  -o $@ $(LDLIBS)
mst-trunc: mst.c $(DEPS)
	$(CC) $(CFLAGS) -DTRUNC_ALG $<  -o $@ $(LDLIBS)
mst-trunc_prefetch: mst.c $(DEPS) 
	$(CC) $(CFLAGS) -DTRUNC_ALG -DPREFETCHER $<  -o $@ $(LDLIBS)
mst-trunc_prefetch_aux: mst.c $(DEPS) 
	$(CC) $(CFLAGS) -DTRUNC_ALG -DPREFETCHER -DAUX_PREFETCH $<  -o $@ $(LDLIBS)

a-star: a-star.c $(DEPS)
	$(CC) $(CFLAGS) $<  -o $@ $(LDLIBS) -lm
a-star_prefetch: a-star.c $(DEPS) 
	$(CC) $(CFLAGS) -DPREFETCHER $<  -o $@ $(LDLIBS) -lm
a-star_prefetch_aux: a-star.c $(DEPS) 
	$(CC) $(CFLAGS) -DPREFETCHER -DAUX_PREFETCH $<  -o $@ $(LDLIBS) -lm

a-star-vertex: a-star.c $(DEPS)
	$(CC) $(CFLAGS) -DTGT_VERTEX $<  -o $@ $(LDLIBS) -lm
a-star-vertex_prefetch: a-star.c $(DEPS) 
	$(CC) $(CFLAGS) -DTGT_VERTEX -DPREFETCHER $<  -o $@ $(LDLIBS) -lm
a-star-vertex_prefetch_aux: a-star.c $(DEPS) 
	$(CC) $(CFLAGS) -DTGT_VERTEX -DPREFETCHER -DAUX_PREFETCH $<  -o $@ $(LDLIBS) -lm

a-star_csr: a-star_csr.c $(DEPS)
	$(CC) $(CFLAGS) -DTGT_VERTEX $<  -o $@ $(LDLIBS) -lm
a-star_csr_prefetch_aux: a-star_csr.c $(DEPS)
	$(CC) $(CFLAGS) -DTGT_VERTEX -DPREFETCHER -DAUX_PREFETCH $<  -o $@ $(LDLIBS) -lm

a-star-trunc: a-star.c $(DEPS)
	$(CC) $(CFLAGS) -DTRUNC_ALG $<  -o $@ $(LDLIBS) -lm
a-star-trunc_prefetch: a-star.c $(DEPS) 
	$(CC) $(CFLAGS) -DTRUNC_ALG -DPREFETCHER $<  -o $@ $(LDLIBS) -lm
a-star-trunc_prefetch_aux: a-star.c $(DEPS) 
	$(CC) $(CFLAGS) -DTRUNC_ALG -DPREFETCHER -DAUX_PREFETCH $<  -o $@ $(LDLIBS) -lm

a-star-trunc-vertex: a-star.c $(DEPS)
	$(CC) $(CFLAGS) -DTRUNC_ALG -DTGT_VERTEX $<  -o $@ $(LDLIBS) -lm
a-star-trunc-vertex_prefetch: a-star.c $(DEPS) 
	$(CC) $(CFLAGS) -DTRUNC_ALG -DTGT_VERTEX -DPREFETCHER $<  -o $@ $(LDLIBS) -lm
a-star-trunc-vertex_prefetch_aux: a-star.c $(DEPS) 
	$(CC) $(CFLAGS) -DTRUNC_ALG -DTGT_VERTEX -DPREFETCHER -DAUX_PREFETCH $<  -o $@ $(LDLIBS) -lm


prim_csr: prim_csr.c $(DEPS)
prim_csr_prefetch_aux: prim_csr.c $(DEPS) 
	$(CC) $(CFLAGS) -DPREFETCHER -DAUX_PREFETCH $<  -o $@ $(LDLIBS)
prim-trunc: prim.c $(DEPS)
	$(CC) $(CFLAGS) -DTRUNC_ALG $<  -o $@ $(LDLIBS)
prim-trunc_prefetch: prim.c $(DEPS) 
	$(CC) $(CFLAGS) -DTRUNC_ALG -DPREFETCHER $<  -o $@ $(LDLIBS)
prim-trunc_prefetch_aux: prim.c $(DEPS) 
	$(CC) $(CFLAGS) -DTRUNC_ALG -DPREFETCHER -DAUX_PREFETCH $<  -o $@ $(LDLIBS)


scc: scc.c $(DEPS)
wcc: wcc.c $(DEPS)


wcc_prefetch: wcc.c $(DEPS) 
	$(CC) $(CFLAGS) -DPREFETCHER $<  -o $@ $(LDLIBS)


scc2: scc2.c $(DEPS)
scc2_prefetch: scc2.c $(DEPS) 
	$(CC) $(CFLAGS) -DPREFETCHER $<  -o $@ $(LDLIBS)

scc_csr: scc_csr.c $(DEPS)
scc_csr_prefetch: scc_csr.c $(DEPS)
	$(CC) $(CFLAGS) -DPREFETCHER $<  -o $@ $(LDLIBS)


pagerank: pagerank.c $(DEPS)
pagerank_csr: pagerank_csr.c $(DEPS)
pagerank_prefetch: pagerank.c $(DEPS) 
	$(CC) $(CFLAGS) -DPREFETCHER $<  -o $@ $(LDLIBS)
pagerank_csr_prefetch: pagerank_csr.c $(DEPS)
	$(CC) $(CFLAGS) -DPREFETCHER $<  -o $@ $(LDLIBS)


conductance: conductance.c $(DEPS)
conductance_prefetch: conductance.c $(DEPS) 
	$(CC) $(CFLAGS) -DPREFETCHER $<  -o $@ $(LDLIBS)
mis: mis.c $(DEPS)
mis_prefetch: mis.c $(DEPS) 
	$(CC) $(CFLAGS) -DPREFETCHER $<  -o $@ $(LDLIBS)

sim_csr: sim_csr.c $(DEPS)
sim_csr_prefetch: sim_csr.c $(DEPS) 
	$(CC) $(CFLAGS) -DPREFETCHER $<  -o $@ $(LDLIBS)

seq_read_benchmark: read_benchmark.c $(DEPS)
	$(CC) $(CFLAGS) $< -o $@ $(LDLIBS)
seq_read_prefetch_benchmark: read_prefetch_benchmark.c $(DEPS) 
	$(CC) $(CFLAGS) $<  -o $@ $(LDLIBS)
seq_checksum_benchmark: checksum_benchmark.c $(DEPS)
	$(CC) $(CFLAGS) $< -o $@ $(LDLIBS)
seq_checksum_prefetch_benchmark: checksum_prefetch_benchmark.c $(DEPS) 
	$(CC) $(CFLAGS) $<  -o $@ $(LDLIBS)


rand_read_benchmark: read_benchmark.c $(DEPS)
	$(CC) $(CFLAGS) -DREAD_RAND $< -o $@ $(LDLIBS)
rand_read_prefetch_benchmark: read_prefetch_benchmark.c $(DEPS) libprefetcher.so
	$(CC) $(CFLAGS) -DREAD_RAND $<  -o $@ $(LDLIBS)
rand_checksum_benchmark: checksum_benchmark.c $(DEPS)
	$(CC) $(CFLAGS) -DREAD_RAND $< -o $@ $(LDLIBS)
rand_checksum_prefetch_benchmark: checksum_prefetch_benchmark.c $(DEPS) libprefetcher.so
	$(CC) $(CFLAGS) -DREAD_RAND $<  -o $@ $(LDLIBS)


prefetcher.o: prefetcher.c $(DEPS)
	$(CC) $(CFLAGS) -c -fpic $< -o $@
libprefetcher.so: prefetcher.o
	$(CC) -shared $(CFLAGS) -o libprefetcher.so\
	 prefetcher.o -lc -lpthread

prefetcher2.o: prefetcher2.c $(DEPS)
	$(CC) $(CFLAGS) $(XLDLIBS) -DNDEBUG -DPREFETCHER2 -c -fpic $< -o $@ 
libprefetcher2.so: prefetcher2.o
	$(CC) prefetcher2.o $(XLDLIBS) $(CFLAGS) -DNDEBUG -DPREFETCHER2 -lpthread -lc -shared -o libprefetcher2.so 
	



clean:
	rm -f $(TARGETS) $(OBSOLETE_TARGETS) *.o

