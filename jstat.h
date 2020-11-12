struct hsperfdata_prologue
{
	unsigned int	magic;			/* magic number - 0xcafec0c0 */
	unsigned int	entry_offset;	/* offset of the first PerfDataEntry */
	unsigned int	num_entries;	/* number of allocated PerfData entries */
};

struct hsperf_file
{
	int fd;
	char *data;
	unsigned int pid;
	unsigned long int st_size;
};

struct gc_metrics
{
	unsigned long int	current_heap_capacity ;
	unsigned long int	current_heap_usage ;
	unsigned long int	young_gc_cnt;
	unsigned long int	final_gc_cnt;
	unsigned int		pid;
	double				young_gc_time;
	double				final_gc_time;
	double				total_gc_time;
};

int open_jstat(struct hsperf_file*, char*, unsigned int);
void close_jstat(struct hsperf_file*);
int jstat(struct gc_metrics*, struct hsperf_file*);
