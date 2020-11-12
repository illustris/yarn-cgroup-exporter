#include "jstat.h" // for struct gc_metrics

#ifndef yarn_structs_h
#define yarn_structs_h

struct app
{
	unsigned long long int cluster_timestamp;
	unsigned int id;
	char user[64];
	char name[128];
	char queue[128];
	unsigned long long int started_time;
	char type[32];
};

struct cnt
{
	unsigned int epoch;
	unsigned long long int cluster_timestamp;
	unsigned int app_id;
	unsigned int attempt_id;
	unsigned int id;

	unsigned long long int mem_allocated;
	unsigned int cores_allocated;
	unsigned long long int started_time;

	unsigned long long int cpu_time;
	unsigned long long int rss;

	struct gc_metrics gcm;
};

#endif