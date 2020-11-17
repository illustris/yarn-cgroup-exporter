#include <stdio.h>
#include "yarn_structs.h"

#ifndef debug_h
#define debug_h

#define DEBUG 0
#define debug_print(fmt, ...) \
	do { if (DEBUG) fprintf(stderr, fmt, ##__VA_ARGS__); fflush(stderr); } while (0)

#define VERBOSE 0
#define debug_print_verbose(fmt, ...) \
	do { if (VERBOSE) fprintf(stderr, fmt, ##__VA_ARGS__); fflush(stderr); } while (0)

void printapp(struct app a);
void printcnt(struct cnt c);
int enable_core_dump();

#endif