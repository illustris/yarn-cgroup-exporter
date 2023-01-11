#include "debug.h"
#include <sys/resource.h>

void printapp(struct app a)
{
	printf("struct app\n{\n\tunsigned long long int cluster_timestamp = %llu;\n\tunsigned int app_id = %u;\n\tchar user[64] = %s;\n\tchar name[128] = %s;\n\tchar queue[128] = %s;\n\tunsigned long long int started_time = %llu;\n\tchar type[32] = %s;\n}\n\n",
	a.cluster_timestamp,a.id,a.user,a.name,a.queue,a.started_time,a.type);
}

void printcnt(struct cnt c)
{
	printf("container_e%u_%llu_%04u_%02u_%06u\n",c.epoch,c.cluster_timestamp,c.app_id,c.attempt_id,c.id);
	printf("struct cnt\n{\n\tunsigned int epoch = %u;\n\tunsigned long long int cluster_timestamp = %llu;\n\tunsigned int app_id = %u;\n\tunsigned int attempt_id = %u;\n\tunsigned int id = %u;\n\tunsigned long long int mem_allocated = %llu;\n\tunsigned int cores_allocated = %u;\n\tunsigned long long int started_time = %llu;\n\tcpu_time = %llu\n\tunsigned long long int rss = %llu",
	c.epoch,c.cluster_timestamp,c.app_id,c.attempt_id,c.id,c.mem_allocated,c.cores_allocated,c.started_time,c.cpu_time,c.rss);
	printf("\n\tstruct gc_metrics gcm =\n\t{\n\t\tunsigned long int	current_heap_capacity = %lu\n\t\tunsigned long int	current_heap_usage = %lu\n\t\tunsigned long int	young_gc_cnt = %lu\n\t\tunsigned long int	final_gc_cnt = %lu\n\t\tunsigned int		pid = %u\n\t\tdouble				young_gc_time = %f\n\t\tdouble				final_gc_time = %f\n\t\tdouble				total_gc_time = %f\n\t}\n}\n\n",
	c.gcm.current_heap_capacity,c.gcm.current_heap_usage,c.gcm.young_gc_cnt,c.gcm.final_gc_cnt,c.gcm.pid,c.gcm.young_gc_time,c.gcm.final_gc_time,c.gcm.total_gc_time);
}

int enable_core_dump(){
	struct rlimit corelim;

	corelim.rlim_cur = RLIM_INFINITY;
	corelim.rlim_max = RLIM_INFINITY;

	return (0 == setrlimit(RLIMIT_CORE, &corelim));
}