#ifndef _PREFETCHER_
#define _PREFETCHER_

/* Inflight targets -- tuned for Sakura */
#define DEFAULT_RANDOM_IFT 32
#define DEFAULT_SEQUENTIAL_IFT 512

/* possibly suboptimal for powers of two */
#define HASH_MODULO(_p, _size) ((_p)%(_size))

typedef void (*random_lookahead)(unsigned long *laf, 
				 unsigned long laf_size,
				 unsigned long ift);
typedef unsigned long (*sequential_lookahead)(unsigned long *aux_offset);
extern void init_prefetcher(random_lookahead random_scan_function,
			    sequential_lookahead sequential_scan_function);
extern void prefetcher_set_aux_fd(int aux_fd);
extern void launch_prefetch_thread(int fd);
extern void terminate_prefetch_thread();
extern void destroy_prefetcher();


#endif
