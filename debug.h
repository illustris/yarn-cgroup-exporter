#define DEBUG 0
#define debug_print(fmt, ...) \
	do { if (DEBUG) fprintf(stderr, fmt, ##__VA_ARGS__); fflush(stderr); } while (0)

#define VERBOSE 0
#define debug_print_verbose(fmt, ...) \
	do { if (VERBOSE) fprintf(stderr, fmt, ##__VA_ARGS__); fflush(stderr); } while (0)
