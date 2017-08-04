#ifndef _MISC_UTILS_
#define _MISC_UTILS_

#define _LARGEFILE64_SOURCE
#include<sys/types.h>
#ifndef __APPLE__
#define _GNU_SOURCE
#include<pthread.h>
#undef  _GNU_SOURCE
#include<sys/stat.h>
#else
#define O_LARGEFILE 0
#define MAP_ANONYMOUS MAP_ANON
#include<pthread.h>
#endif
#include<unistd.h>
#include<fcntl.h>
#include<stdio.h>
#include<errno.h>
#include<stdlib.h>
#include<string.h>
#include<assert.h>
#include<sys/types.h>
#include<sys/resource.h>
#include<sys/mman.h>
#include<unistd.h>
#include<limits.h>

#ifndef __LP64__
#error Only compile with 64-bit
#endif


static unsigned long mlocked_memory = 0;
/* Bind the calling thread to a processor
 * Does a 1-1 map of thread_id
 * to available processors
 */
#ifndef __APPLE__
/* Master on core 1 (not 0) and prefetcher on core 2 */
#define CPUSET_MASTER 2
#define CPUSET_PREFETCHER 4
extern void register_mlocked_memory(unsigned long size)
{
  mlocked_memory += size;
}

void unregister_mlocked_memory(unsigned long size)
{
  mlocked_memory -= size;
}

void print_mlocked_memory() 
{
	fprintf(stderr, "Inflating balloon, mlocked memory = %lu bytes\n",
          mlocked_memory);
	if(getenv("BALLOON_CHK") != NULL) {
    exit(0);
  }
}

static void bind_master()
{
  int retval;
  unsigned long mask = CPUSET_MASTER;
  retval = pthread_setaffinity_np(pthread_self(), sizeof(unsigned long), &mask);
  if(retval) {
    perror("failed to bind master:");
  }
}
static void bind_prefetcher()
{
  int retval;
  unsigned long mask = CPUSET_PREFETCHER;
  retval = pthread_setaffinity_np(pthread_self(), sizeof(unsigned long), &mask);
  if(retval) {
    perror("failed to bind prefetcher:");
  }
}

#else
static void bind_master()
{
  fprintf(stderr, "Warning ! Binding not supported on Mac OSX\n");
}
static void bind_prefetcher()
{
  fprintf(stderr, "Warning ! Binding not supported on Mac OSX\n");
}
#endif

#define ASSUME_PAGE_SIZE 4096
#define ASSUME_PAGE_SHIFT 12

/* Timing related utils */
#include<sys/time.h>
static unsigned long get_current_rtc()
{
  struct timeval tm;
  gettimeofday(&tm, NULL);
  return tm.tv_sec*1000000 + tm.tv_usec;
}

static unsigned long get_rtc_diff(unsigned long final, unsigned long init)
{
  return final - init;
}

static unsigned long get_timeval_diff(struct timeval final, struct timeval init)
{
  return ((final.tv_sec*1000000 + final.tv_usec) -
	  (init.tv_sec*1000000 + init.tv_usec));
}

#define CLOCK_START(_counter) do {		\
    (_counter) = get_current_rtc();		\
  }while(0)

#define CLOCK_STOP(_counter) do {			\
    unsigned long _temp_clock;				\
    _temp_clock = get_current_rtc();			\
    (_counter) = get_rtc_diff(_temp_clock, (_counter));	\
  }while(0)

void print_rusage_stats(FILE *stream,
			struct rusage *begin,
			struct rusage *end)
{
  fprintf(stream, "RU_USER %ld\n",
	  get_timeval_diff(end->ru_utime, begin->ru_utime));
  fprintf(stream, "RU_SYS %ld\n",
	  get_timeval_diff(end->ru_stime, begin->ru_stime));
  fprintf(stream, "RU_MAXRSS_KB %ld\n",
	  end->ru_maxrss);
  fprintf(stream, "RU_INBLOCK %ld\n",
	  end->ru_inblock - begin->ru_inblock);
  fprintf(stream, "RU_MAJFLT %ld\n",
	  end->ru_majflt - begin->ru_majflt);
}


/* Useful macros */
#define ABS_DIFF(_me, _other) ((_me) > (_other) ? ((_me) - (_other)) : ((_other) - (_me)))
#define SIGN_DIFF(_me, _other) ((_me) > (_other) ? 1:0)
#define MIN(_A,_B) (_A<_B ? _A : _B)



static void *map_anon_memory(unsigned long size, char *fail_string)
{
  void *space = mmap(NULL, size, PROT_READ|PROT_WRITE,
		     MAP_ANONYMOUS|MAP_SHARED, -1, 0);
  if(space == MAP_FAILED) {
    perror("mmap failed:");
    fprintf(stderr, "info %s\n", fail_string);
    exit(-1);
  }
  /* Anonymous memory is mlocked to achieve
   * semi-external memory IO performance
   */
  if(mlock(space, size) < 0) {
    perror("mlock failed:");
    fprintf(stderr, "info %s\n", fail_string);
    exit(-1);
  }
  register_mlocked_memory(size);
  return space;
}



#define RASP_THRESHOLD 1000

#endif
