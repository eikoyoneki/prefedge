#include "misc_utils.h"
#include "prefetcher.h"
#include "balloon.h"

/* 4 GB test file */
#define FILE_SIZE (1UL << 32)
#define MASK ((FILE_SIZE/ASSUME_PAGE_SIZE) - 1)
/* A period of FILE_SIZE/ASSUME_PAGE_SIZE
 * and random enough to beat the OS
 * prefetcher is good enough
 */

volatile unsigned long rng_state;
#ifdef READ_RAND
#define LCG_NEXT(_n) ((1103515245*(_n) + 12345) & MASK)
#else
#define LCG_NEXT(_n) (((_n) + 1) & MASK)
#endif

#ifdef READ_RAND

void prefetcher_callback(unsigned long *laf,
			 unsigned long laf_size,
			 unsigned long ift)
{
  unsigned long trand = rng_state;
  unsigned long entries = 0;
  while(entries != ift) {
    trand = LCG_NEXT(trand);
    if(laf[HASH_MODULO(trand, laf_size)] != trand) {
      laf[HASH_MODULO(trand, laf_size)] = trand;
      entries++;
    }
  }
}

#else

unsigned long prefetcher_callback()
{
  unsigned long trand = rng_state;
  trand = LCG_NEXT(trand);
  return trand;
}

#endif

int main(int argc, char *argv[])
{
  int fd;
  volatile char *space;
  char byte;
  unsigned long i, j, size;
  unsigned long runtime;
  if(argc < 3) {
    fprintf(stderr, "Usage %s filename lcg_seed\n", argv[0]);
    exit(-1);
  }
  bind_master();
#ifdef READ_RAND
  init_prefetcher(prefetcher_callback, NULL);
#else
  init_prefetcher(NULL, prefetcher_callback);
#endif
  balloon_init();
  balloon_inflate();
  fd = open(argv[1], O_RDONLY|O_LARGEFILE);
  if(fd == -1) {
    perror("Unable to open file:");
    exit(-1);
  }
  size = lseek(fd, 0, SEEK_END);
  lseek(fd, 0, SEEK_SET);
  assert(size >= FILE_SIZE);
#ifndef __APPLE__
  int adv_ret = posix_fadvise(fd, 0, FILE_SIZE, POSIX_FADV_RANDOM);
  if(adv_ret < 0) {
    perror("posix fadv random failed:");
    exit(-1);
  }
#endif
  space = (char *)
    mmap(0, size, PROT_READ, MAP_FILE|MAP_SHARED, fd, 0);
  if(space == MAP_FAILED) {
    perror("Unable to map file:");
    exit(-1);
  }
  /* test goes here */
  rng_state = atol(argv[2]);
  launch_prefetch_thread(fd);
  CLOCK_START(runtime);
  for(i=0;i<(FILE_SIZE/ASSUME_PAGE_SIZE);i++) {
    rng_state = LCG_NEXT(rng_state);
    /* Checksum the zero page */
    byte = 0;
    for(j=0;j<ASSUME_PAGE_SIZE;j++) {
      byte |= space[rng_state*ASSUME_PAGE_SIZE];
    }
    assert(byte == 0);
  }
  CLOCK_STOP(runtime);
  balloon_deflate();
  terminate_prefetch_thread();
  munmap((void*)space, size);
  close(fd);
  destroy_prefetcher();
  printf("SYNC_CHECKSUM_OP_RATE = %lf\n", 
	 ((double)FILE_SIZE/ASSUME_PAGE_SIZE)*1000000.0/runtime);
  return 0;
}
