#include "graph_defs.h"
#include "prefetcher.h"
#include <limits.h>

typedef struct bfs_metadata_st {
	char touched;
	volatile unsigned long queue_next;
} bfs_metadata_t;

static volatile unsigned long queue_head = ULONG_MAX;
static volatile unsigned long vertex_position = 0;
static bfs_metadata_t *metadata;
static csr_t * volatile graph;
unsigned long MAX_CACHE = ULONG_MAX;
long MIN_CACHE = 0;
unsigned long visited = 0;

void prefetcher_random_callback(unsigned long *laf, unsigned long laf_size,
		unsigned long ift) {
	static unsigned long old_hoq = ULONG_MAX;
	unsigned long current_hoq = ULONG_MAX;
	static unsigned long ra_depth = 0;
	static char preload = 0;
	static long pf_visited = 0;
	unsigned long entries = 0;
	/* Fill in inner-loop entries from BFS queue */
	/*
	 if ((preload == 0) && (ra_depth > MAX_CACHE)) {
	 preload = 1;
	 current_hoq = ULONG_MAX;
	 }
	 */

	current_hoq = old_hoq;
	if ((current_hoq == ULONG_MAX)
			|| (((signed long) (pf_visited - visited)) > MIN_CACHE)/*|| (ra_depth > MIN_CACHE)*/) {
		current_hoq = queue_head;
		pf_visited = visited;
		//    ra_depth = 0;
	}
	//  if (((signed long)(pf_visited - visited)) > MIN_CACHE) return;
	/*  if(current_hoq != ULONG_MAX) {
	 current_hoq = metadata[current_hoq].queue_next;
	 }
	 */
	while (entries != ift && current_hoq != ULONG_MAX) {
		unsigned long page = graph->index[current_hoq];
		unsigned long end = graph->index[current_hoq + 1];
		page = page >> (ASSUME_PAGE_SHIFT + 3); /* offset is in bits ! */
		end = end >> (ASSUME_PAGE_SHIFT + 3);
		//    if(laf[HASH_MODULO(page, laf_size)] != page) {
		//      laf[HASH_MODULO(page, laf_size)] = page;
		//    for (; page <= end; page++) {
		//	if (entries==ift) break;
		laf[entries] = page;
		if (end > page)
			laf[entries + (2 * laf_size)] = end - page;
		entries++;
		//    }
		//    }
		old_hoq = current_hoq;
		current_hoq = metadata[current_hoq].queue_next;
		pf_visited++;
	}
	ra_depth += entries;
}

unsigned long prefetcher_sequential_callback(unsigned long* aux_offset) {
	unsigned long offset = graph->index[vertex_position];
	return offset >> (ASSUME_PAGE_SHIFT + 3);
}

unsigned long alist_entries_seen = 0;
// #pragma omp threadprivate(current_vertex)

unsigned long total_queue_demands = 0;
unsigned long queue_above_threshold = 0;
unsigned long queue_length = 0;
/* returns number of connected components */
static unsigned long bfs(csr_t *graph, unsigned long start_node) {
	unsigned long i;
	unsigned long components = 0;
	unsigned long queue_tail = ULONG_MAX;
	unsigned long nq_head = ULONG_MAX;
	unsigned long nq_tail = ULONG_MAX;

	char* finished_flag = malloc(sizeof(char) * omp_get_num_threads());

	unsigned long time_comp, time_giant = 0, id_giant;
	i = start_node;

	do {
		int level = 0;
		vertex_position = i;
		if (metadata[i].touched == 0) {
			CLOCK_START(time_comp);
			metadata[i].touched = 1;
			visited++;
			components++;
			//fprintf(stderr, "C %ld %d\n", i, omp_get_thread_num());
		} else {
			i++;
			if (i >= graph->vertex_cnt)
				i = 0;
			continue;
		}
		while (1) {
			unsigned long current_vertex;
			level++;
			memset(finished_flag, 1, omp_get_num_threads());
#pragma omp parallel for
			for (current_vertex = 0; current_vertex < graph->vertex_cnt;
					current_vertex++) {
				if (metadata[current_vertex].touched != level)
					continue;
				csr_edge_iterator_t iter;
				csr_init_edge_iterator(graph, current_vertex, &iter);
				while (csr_iter_step(graph, &iter) == 0) {
					if (!iter.incoming) {
						unsigned long target = iter.neighbour;
#pragma omp critical (atomicset)
						{
							if (metadata[target].touched == 0) {
								metadata[target].touched = level + 1;
								finished_flag[omp_get_thread_num()] = 0;
								visited++;
								//fprintf(stderr, "V %ld %d\n", target, omp_get_thread_num());
							}
						}
					}
				}
			}

			int j;
			for (j = 0; j < omp_get_num_threads(); j++) {
				if (finished_flag[i] == 0) {
					break;
				}
			}
			if (j == omp_get_num_threads()) break;
		}

		CLOCK_STOP(time_comp);
		//fprintf(stderr, "%ld\n", time_comp);
		if (time_comp > time_giant) {
			time_giant = time_comp;
			id_giant = i;
			printf("Visited %ld\n", visited);
			return 1;
		}
		i++;
		if (i >= graph->vertex_cnt) {
			i = 0;
		}

	} while (i != start_node);
	//  fprintf(stderr, "%ld %ld\n", visited, graph->vertex_cnt);
	assert(visited == graph->vertex_cnt);
	printf("TIME GIANT COMP %lu\n", time_giant);
	printf("ID GIANT COMP %lu\n", id_giant);
	return components;
}

int main(int argc, char **argv) {
	unsigned long time_bfs, time_total, components;
	CLOCK_START(time_total);
	if (argc < 3) {
		fprintf(stderr, "Usage %s graph_name root_id\n", argv[0]);
		exit(-1);
	}
#ifdef PREFETCHER
	char *env_var;
	env_var = getenv("CMAX");
	if(env_var != NULL) {
		MAX_CACHE = atol(env_var);
	}
	env_var = getenv("CMIN");
	if(env_var != NULL) {
		MIN_CACHE = atol(env_var);
	}

	bind_master();
	init_prefetcher(prefetcher_random_callback,
			NULL);
	//		  prefetcher_sequential_callback);
#endif
	graph = open_csr(argv[1]);
	metadata = (bfs_metadata_t*) map_anon_memory(
			graph->vertex_cnt * sizeof(bfs_metadata_t), "vertex metadata");
	//balloon_inflate(); /* Simulate semi-em conditions */
	print_mlocked_memory();
	unsigned long root_id = atol(argv[2]);
	assert(root_id < graph->vertex_cnt);
	/* Perhaps mmap /dev/null instead ? */
	memset(metadata, 0, graph->vertex_cnt * sizeof(bfs_metadata_t));
#ifdef PREFETCHER
	launch_prefetch_thread(graph->fd_calist);
#endif
	struct rusage ru_begin;
	getrusage(RUSAGE_SELF, &ru_begin);
	CLOCK_START(time_bfs);
	components = bfs(graph, root_id);
	CLOCK_STOP(time_bfs);
	struct rusage ru_end;
	getrusage(RUSAGE_SELF, &ru_end);
#ifdef PREFETCHER
	terminate_prefetch_thread();
	destroy_prefetcher();
#endif
	munmap(metadata, graph->vertex_cnt * sizeof(bfs_metadata_t));
	close_csr(graph);
	CLOCK_STOP(time_total);
	printf("COMPONENTS %lu\n", components);
	printf("TIME BFS %lu\n", time_bfs);
	printf("TIME TOTAL %lu\n", time_total);
	print_rusage_stats(stdout, &ru_begin, &ru_end);
	printf("F_THRESHOLD %f\n",
			((double) queue_above_threshold) / total_queue_demands);
	return 0;
}
