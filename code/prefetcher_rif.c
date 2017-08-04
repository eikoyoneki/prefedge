#include<stdlib.h>
#include<string.h>
#include<pthread.h>
#include "misc_utils.h"
#include"prefetcher.h"

static pthread_t prefetch_thread;
static volatile int prefetch_fd;
static volatile int aux_fd = -1;
#define RUNNING 1
#define TERMINATE 2
static volatile unsigned long prefetch_thread_state = 0;
/***** Random access prefetcher *****/
static random_lookahead random_lookahead_callback = NULL;
static unsigned long random_ift = DEFAULT_RANDOM_IFT;
/* lookahead filter (laf) */
static unsigned long laf_size = 2*DEFAULT_RANDOM_IFT;
static unsigned long *laf; 
/* random inflight filter (rif) */
#define RIF_SIZE 1024
#define RIF_HASH(_p) ((_p) & (RIF_SIZE - 1))
static unsigned long *rif;
/* Sequential access prefetcher */
static sequential_lookahead sequential_lookahead_callback = NULL;
static unsigned long sequential_ift = DEFAULT_SEQUENTIAL_IFT;
static unsigned long alaf = 0;

#ifndef __APPLE__

static void *prefetcher(void *arg)
{
  unsigned long i, retval, offset, aux_offset;
  unsigned long last_sequential_offset = 0; 
  unsigned long next_sequential_offset;
  bind_prefetcher();
  memset(rif, 0, RIF_SIZE*sizeof(unsigned long));
  while(prefetch_thread_state != TERMINATE) {
    memset(laf, 0, laf_size*sizeof(unsigned long));
    /* Issue random prefetch */
    if(random_lookahead_callback != NULL) {
      if(aux_fd != -1) {
	random_lookahead_callback(laf, laf_size, random_ift/2); 
      }
      else {
	random_lookahead_callback(laf, laf_size, random_ift); 
      }
      unsigned long entries = 0;
      for(i=0;i<laf_size;i++) {
	offset = laf[i];
	if(offset) {
	  if(rif[RIF_HASH(offset)] != offset) {
	    entries++;
	    rif[RIF_HASH(offset)] = offset;
	    retval = posix_fadvise(prefetch_fd,
				   offset*ASSUME_PAGE_SIZE, 
				   ASSUME_PAGE_SIZE, 
				   POSIX_FADV_WILLNEED);
	    if(retval) {
	      perror("Prefetch failed: ");
	      exit(-1);
	    }
	    if(aux_fd != -1) {
	      aux_offset = laf[i + laf_size];
	      retval = posix_fadvise(aux_fd,
				     aux_offset*ASSUME_PAGE_SIZE, 
				     ASSUME_PAGE_SIZE, 
				     POSIX_FADV_WILLNEED);
	      if(retval) {
		perror("Prefetch failed: ");
		exit(-1);
	      }
	    }
	  }
	}
	if ((alaf == 1) && (i == laf_size-1))
		laf_size *= 2; random_ift *= 2;
      }
    }
    /* Issue sequential prefetch */
    if(sequential_lookahead_callback != NULL) {
      unsigned long aux_offset;
      next_sequential_offset = sequential_lookahead_callback(&aux_offset);
      unsigned long diff = ABS_DIFF(next_sequential_offset,
				    last_sequential_offset);
      if(aux_fd != -1) {
	if(diff >= (sequential_ift/2)) {
	  last_sequential_offset = next_sequential_offset;
	  /* Scrunch into two calls */
	  retval = posix_fadvise(prefetch_fd,
				 last_sequential_offset*ASSUME_PAGE_SIZE,
				 ASSUME_PAGE_SIZE*(sequential_ift/2),
				 POSIX_FADV_WILLNEED);
	  retval = posix_fadvise(aux_fd,
				 aux_offset*ASSUME_PAGE_SIZE,
				 ASSUME_PAGE_SIZE*(sequential_ift/2),
				 POSIX_FADV_WILLNEED);
	}
      }
      else {
	if(diff >= sequential_ift) {
	  last_sequential_offset = next_sequential_offset;
	  /* Scrunch into one call */
	  retval = posix_fadvise(prefetch_fd,
				 last_sequential_offset*ASSUME_PAGE_SIZE,
				 ASSUME_PAGE_SIZE*sequential_ift,
				 POSIX_FADV_WILLNEED);
	}
      }
    }
  }
  return NULL;
}

#else

static void *prefetcher(void *arg)
{
  fprintf(stderr, "Warning: no prefetch support yet on Mac OSX");
  while(prefetch_thread_state != TERMINATE) {
  }
}
#endif

void init_prefetcher(random_lookahead random_scan_function,
		     sequential_lookahead sequential_scan_function)
{
  /* Pull in necessary environment variables */
  char *env_var;
  env_var = getenv("SEQ_IFT");
  if(env_var != NULL) {
    sequential_ift = atol(env_var);
  }
  env_var = getenv("RAND_IFT");
  if(env_var != NULL) {
    random_ift = atol(env_var);
    laf_size = 2*random_ift;
  }

  env_var = getenv("ALAF");
  if(env_var != NULL) {  
    alaf = atol(env_var);
  }
  assert(random_lookahead_callback == NULL);
  random_lookahead_callback = random_scan_function;
  assert(sequential_lookahead_callback == NULL);
  sequential_lookahead_callback = sequential_scan_function;

  /* Note: the factor of 2 below is to take aux pointers into account */
  laf = (unsigned long *)malloc(2 * laf_size * sizeof(unsigned long));
  assert(laf != NULL);
  rif = (unsigned long *)malloc(RIF_SIZE*sizeof(unsigned long));
  assert(rif != NULL);
  /* Let the user know what they are getting */
  fprintf(stderr, "Prefetcher initialised with rand_ift = %lu seq_ift = %lu\n",
	  random_ift, sequential_ift);
}

void prefetcher_set_aux_fd(int fd)
{
  assert(aux_fd == -1);
  aux_fd = fd;
}

void launch_prefetch_thread(int fd)
{
  int launch_code;
  assert(prefetch_thread_state == 0);
  prefetch_thread_state = RUNNING;
  prefetch_fd = fd;
  launch_code = pthread_create(&prefetch_thread,
			       NULL, prefetcher, NULL);
  if(launch_code) {
    perror("Failed to launch prefetch thread:");
  }
}

void terminate_prefetch_thread()
{
  assert(prefetch_thread_state == RUNNING);
  prefetch_thread_state = TERMINATE;
  pthread_join(prefetch_thread, NULL);
  prefetch_thread_state = 0;
}

void destroy_prefetcher()
{
  assert(prefetch_thread_state == 0);
  free(laf);
  free(rif);
}
