/* Copyright (C) 2009-2010 The Trustees of Indiana University.             */
/*                                                                         */
/* Use, modification and distribution is subject to the Boost Software     */
/* License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at */
/* http://www.boost.org/LICENSE_1_0.txt)                                   */
/*                                                                         */
/*  Authors: Jeremiah Willcock                                             */
/*           Andrew Lumsdaine                                              */

#ifndef GRAPH_GENERATOR_H
#define GRAPH_GENERATOR_H

#include "user_settings.h"
#include <stdlib.h>
#include <stdint.h>

#include <stdio.h>

#include<sys/time.h>

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <inttypes.h>

#ifdef GRAPH_GENERATOR_OMP
#include <omp.h>
#endif


#ifdef __cplusplus
extern "C" {
#endif

unsigned long *remap_vector;

#ifdef GENERATOR_USE_PACKED_EDGE_TYPE

typedef struct packed_edge {
  uint32_t v0_low;
  uint32_t v1_low;
  uint32_t high; /* v1 in high half, v0 in low half */
} packed_edge;

static inline int64_t get_v0_from_edge(const packed_edge* p) {
  return (p->v0_low | ((int64_t)((int16_t)(p->high & 0xFFFF)) << 32));
}

static inline int64_t get_v1_from_edge(const packed_edge* p) {
  return (p->v1_low | ((int64_t)((int16_t)(p->high >> 16)) << 32));
}

static inline void write_edge(packed_edge* p, int64_t v0, int64_t v1) {
  p->v0_low = (uint32_t)v0;
  p->v1_low = (uint32_t)v1;
  p->high = ((v0 >> 32) & 0xFFFF) | (((v1 >> 32) & 0xFFFF) << 16);
}

#else

typedef struct packed_edge {
  int64_t v0;
  int64_t v1;
} packed_edge;

static inline int64_t get_v0_from_edge(const packed_edge* p) {
  return p->v0;
}

static inline int64_t get_v1_from_edge(const packed_edge* p) {
  return p->v1;
}


#define MAX_LINES 131072
#define WIDTH 25
#ifdef _OPENMP
static inline void write_edge(packed_edge* p, int64_t v0, int64_t v1) {
	static long buf;
#pragma omp threadprivate(buf)
	static char* buffile = NULL;	
	if (buf==0) {
#pragma omp barrier
/* PARALLEL */ 
		//if (buffile) fclose(buffile);\q
		// COPY to db
		if (buffile) { 
			FILE* f = popen("psql graphssd -c \"COPY edge FROM STDIN DELIMITERS ',';\"","w");
			for (int i = 1 + omp_get_thread_num(); i < MAX_LINES * omp_get_num_threads(); i+= omp_get_num_threads())		
				fprintf(f, "%s", buffile + (i*WIDTH));
			fclose(f);
		} else
#pragma omp master
{		
			buffile = (char*) malloc(sizeof(char)*WIDTH*MAX_LINES*omp_get_num_threads());
/*
#pragma omp master
{		
		//if (buffile) fclose(buffile);
		// COPY to db
		if (buffile) { 
			FILE* f = popen("psql graph -c \"COPY edge FROM STDIN DELIMITERS ',';\"","w");
			for (int i = 1; i < MAX_LINES * omp_get_num_threads(); i++) 		
				fprintf(f, "%s", buffile + (i*WIDTH));
			fclose(f);
		} else
			buffile = (char*) malloc(sizeof(char)*WIDTH*MAX_LINES*omp_get_num_threads());
*/
}
#pragma omp barrier
		buf = MAX_LINES-1;

	}
	sprintf(buffile+(buf*WIDTH*omp_get_num_threads())+(omp_get_thread_num()*WIDTH), "%ld, %ld\n", v0, v1);
#pragma omp atomic
	buf--;
//  p->v0 = v0;
//  p->v1 = v1;
}
#else

static inline void write_edge(packed_edge* p, int64_t v0, int64_t v1) {
	static long buf = MAX_LINES;
	static char* buffile = NULL;	
	if (buf==MAX_LINES) {
		if (buffile) { 
			//FILE* f = popen("psql graphssd -c \"COPY edge FROM STDIN DELIMITERS ',';\"","w");
			FILE* f = stdout;			
			for (int i = 0; i < MAX_LINES; i++) {
				fprintf(f, "%s", buffile + (i*WIDTH));
			}
			//fclose(f);
		} else
			buffile = (char*) malloc(sizeof(char)*WIDTH*MAX_LINES);

		buf = 0;

	}
	if (sprintf(buffile+(buf*WIDTH), "%ld \t%ld\n", v0, v1) > WIDTH) printf("aaaah\n");
	buf++;
}

#endif

#endif

/* Generate a range of edges (from start_edge to end_edge of the total graph),
 * writing into elements [0, end_edge - start_edge) of the edges array.  This
 * code is parallel on OpenMP and XMT; it must be used with
 * separately-implemented SPMD parallelism for MPI. */
void generate_kronecker_range(
       const uint_fast32_t seed[5] /* All values in [0, 2^31 - 1) */,
       int logN /* In base 2 */,
       int64_t start_edge, int64_t end_edge /* Indices (in [0, M)) for the edges to generate */,
       packed_edge* edges /* Size >= end_edge - start_edge */
);


#ifdef __cplusplus
}
#endif

#endif /* GRAPH_GENERATOR_H */
