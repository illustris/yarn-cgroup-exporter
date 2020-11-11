#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <curl/curl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <librdkafka/rdkafka.h>

//Container_e{epoch}_{clusterTimestamp}_{appId}_{attemptId}_{containerId}
//Container_{clusterTimestamp}_{appId}_{attemptId}_{containerId}

#define DEBUG 0
#define debug_print(fmt, ...) \
	do { if (DEBUG) fprintf(stderr, fmt, ##__VA_ARGS__); fflush(stderr); } while (0)

#define VERBOSE 0
#define debug_print_verbose(fmt, ...) \
	do { if (VERBOSE) fprintf(stderr, fmt, ##__VA_ARGS__); fflush(stderr); } while (0)

char rm1_url[128];
char rm2_url[128];
char* active_rm;

char cache_path[256] = "/dev/shm/yarn_exporter_cache";
char cgroup_root[64] = "/sys/fs/cgroup/cpu/hadoop-yarn";
unsigned int app_cache_hit = 0;
unsigned int app_cache_miss = 0;
unsigned int cnt_cache_hit = 0;
unsigned int cnt_cache_miss = 0;

unsigned int cache_expiry = 60;

char hsperf_basepath[] = "/tmp/hsperfdata_nobody";

char kafka_buffer[128*1024] = "";
char kafka_brokers[256];
char kafka_topic[32];

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

int open_jstat(struct hsperf_file *hsfile, unsigned int pid)
{
	struct stat mmapstat;
	char path[128];
	sprintf(path, "%s/%u", hsperf_basepath, pid);
	debug_print("open_jstat: Opening PID %u from path %s\n",pid, path);
	stat(path, &mmapstat);
	hsfile->st_size = mmapstat.st_size;
	hsfile->fd = open(path, O_RDONLY);
	hsfile->data = mmap((caddr_t)0, hsfile->st_size, PROT_READ, MAP_SHARED, hsfile->fd, 0);
	if((long int)(hsfile->data) == -1)
	{
		//printf("Failed to open file\n");
		debug_print("open_jstat: Failed to open hsperf file for PID %u from path %s\n",pid, path);
		close(hsfile->fd);
		return 0;
	}
	hsfile->pid = pid;
	return 1;
}

void close_jstat(struct hsperf_file *hsfile)
{
	munmap(hsfile->data, hsfile->st_size);
	close(hsfile->fd);
}

int jstat(struct gc_metrics *gcm, struct hsperf_file *hsfile)
{
	unsigned int magic, entry_offset, num_entries;
	char *entry_base;
	unsigned int entry_length, name_offset, data_offset, vector_length;
	unsigned long int value_long, sun_os_hrt_frequency;
	unsigned long int *value_base;
	unsigned char *key_base;
	char key[64];
	size_t key_length;

	magic = *((unsigned int *)(hsfile->data + 0));
	//printf("%x\n",magic);
	if(magic != 0xc0c0feca)
	{
		debug_print("jstat: Bad magic number for PID %u\n",hsfile->pid);
		return -1;
	}

	entry_offset = *((unsigned int *)(hsfile->data + 24));
	num_entries = *((unsigned int *)(hsfile->data + 28));

	//printf("%x\n%u\n%u\n",magic,entry_offset,num_entries);

	entry_base = hsfile->data+entry_offset;
	for (size_t i=0; i < num_entries;i++)
	{
		// get some stuff from base pointer + offset
		entry_length = *((unsigned int *)(entry_base + 0));
		name_offset = *((unsigned int *)(entry_base + 4));
		data_offset = *((unsigned int *)(entry_base + 16));
		vector_length = *((unsigned int *)(entry_base + 8));

		// get pointer to key
		key_base = entry_base + name_offset;
		key_length = strlen((char *)key_base);
		strncpy(key,key_base,key_length);
		key[key_length]=0;

		// Get pointer to value
		value_base = (unsigned long int *)(entry_base + data_offset);

		// set entry_base to next entry for the next iter
		entry_base = entry_base + entry_length;

		// None of the values we need are vectors
		// Skip if value is a vector
		if(vector_length)
		{
			continue;
		}

		value_long = *((unsigned long int *)value_base);
		// TODO: Find less ugly way to do this without compromising performance
		if(strncmp("sun.gc.collector.0.invocations",key,64) == 0)
		{
			gcm->young_gc_cnt = value_long;
		}
		else if(strncmp("sun.gc.collector.0.time",key,64) == 0)
		{
			gcm->young_gc_time = value_long;
			gcm->total_gc_time += value_long;
		}
		else if(strncmp("sun.gc.collector.1.invocations",key,64) == 0)
		{
			gcm->final_gc_cnt = value_long;
		}
		else if(strncmp("sun.gc.collector.1.time",key,64) == 0)
		{
			gcm->final_gc_time = value_long;
			gcm->total_gc_time += value_long;
		}
		else if(strncmp("sun.gc.generation.0.space.0.capacity",key,64) == 0)
		{
			gcm->current_heap_capacity += value_long;
		}
		else if(strncmp("sun.gc.generation.0.space.0.used",key,64) == 0)
		{
			gcm->current_heap_usage += value_long;
		}
		else if(strncmp("sun.gc.generation.0.space.1.capacity",key,64) == 0)
		{
			gcm->current_heap_capacity += value_long;
		}
		else if(strncmp("sun.gc.generation.0.space.1.used",key,64) == 0)
		{
			gcm->current_heap_usage += value_long;
		}
		else if(strncmp("sun.gc.generation.0.space.2.capacity",key,64) == 0)
		{
			gcm->current_heap_capacity += value_long;
		}
		else if(strncmp("sun.gc.generation.0.space.2.used",key,64) == 0)
		{
			gcm->current_heap_usage += value_long;
		}
		else if(strncmp("sun.gc.generation.1.space.0.capacity",key,64) == 0)
		{
			gcm->current_heap_capacity += value_long;
		}
		else if(strncmp("sun.gc.generation.1.space.0.used",key,64) == 0)
		{
			gcm->current_heap_usage += value_long;
		}
		else if(strncmp("sun.os.hrt.frequency",key,64) == 0)
		{
			sun_os_hrt_frequency = value_long;
		}
		else
			continue;

		//printf("%s: %lu\n",key,value_long);
	}

	gcm->young_gc_time /= sun_os_hrt_frequency;
	gcm->final_gc_time /= sun_os_hrt_frequency;
	gcm->total_gc_time /= sun_os_hrt_frequency;
	gcm->pid = hsfile->pid;

	return 0;
}

struct string
{
	char *ptr;
	size_t len;
};

void init_string(struct string *s)
{
	s->len = 0;
	s->ptr = malloc(s->len+1);
	if (s->ptr == NULL)
	{
		fprintf(stderr, "malloc() failed\n");
		exit(EXIT_FAILURE);
	}
	s->ptr[0] = '\0';
}

size_t writefunc(void *ptr, size_t size, size_t nmemb, struct string *s)
{
	debug_print("writefunc: adding %zu bytes to struct string %p\n",nmemb,s);
	size_t new_len = s->len + size*nmemb;
	s->ptr = realloc(s->ptr, new_len+1);
	if (s->ptr == NULL)
	{
		fprintf(stderr, "realloc() failed\n");
		exit(EXIT_FAILURE);
	}

	memcpy(s->ptr+s->len, ptr, size*nmemb);
	s->ptr[new_len] = '\0';
	s->len = new_len;

	return size*nmemb;
}

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

// Traverse as attempt_id, epoch, cluster_timestamp, app_id, container_id to keep tree small
/*struct cid_trie_node
{
	struct cid_trie_node *children[256];
	struct cnt *c=NULL;
	unsigned char depth=0;
};*/

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

struct app *get_app(unsigned long long int, unsigned int);

void jsoncnt(struct cnt c,char *json)
{
	struct app *a;
	char buf[2048];
	char *buf_ptr;
	buf_ptr = buf;
	a = get_app(c.cluster_timestamp, c.app_id);
	// TODO: this is a temporary fix for the bug introduced by the last commit
	// Need to find out why those changes make get_app on dead containers return NULL
	if(!a)
		a = calloc(1,sizeof(struct app)); // create an empty app
	buf_ptr+=sprintf(buf_ptr,"{\"application_id\":\"application_%llu_%04u\",\"user\":\"%s\",\"name\":\"%s\",\"queue\":\"%s\",\"app_start_time\":%llu,\"type\":\"%s\",",
		a->cluster_timestamp,a->id,a->user,a->name,a->queue,a->started_time,a->type);
	buf_ptr+=sprintf(buf_ptr,"\"container\":\"container_e%u_%llu_%04u_%02u_%06u\",",c.epoch,c.cluster_timestamp,c.app_id,c.attempt_id,c.id);
	buf_ptr+=sprintf(buf_ptr,"\"epoch\":%u,\"cluster_timestamp\":%llu,\"app_id\":%u,\"attempt_id\":%u,\"id\":%u,\"mem_allocated\":%llu,\"cores_allocated\":%u,\"started_time\":%llu,\"cpu_time\":%llu,\"rss\":%llu,",
	c.epoch,c.cluster_timestamp,c.app_id,c.attempt_id,c.id,c.mem_allocated,c.cores_allocated,c.started_time,c.cpu_time,c.rss);
	buf_ptr+=sprintf(buf_ptr,"\"current_heap_capacity\":%lu,\"current_heap_usage\":%lu,\"young_gc_cnt\":%lu,\"final_gc_cnt\":%lu,\"pid\":%u,\"young_gc_time\":%f,\"final_gc_time\":%f,\"total_gc_time\":%f}",
	c.gcm.current_heap_capacity,c.gcm.current_heap_usage,c.gcm.young_gc_cnt,c.gcm.final_gc_cnt,c.gcm.pid,c.gcm.young_gc_time,c.gcm.final_gc_time,c.gcm.total_gc_time);
	free(a); // no memory leaks pls
	strcat(json,buf);
	return;
}

struct stat st = {0};

void initcache()
{
	char path[258];
	if (stat(cache_path, &st) == -1)
		mkdir(cache_path,0755);
	sprintf(path,"%s/applications",cache_path);
	if (stat(path, &st) == -1)
		mkdir(path,0755);
	sprintf(path,"%s/containers",cache_path);
	if (stat(path, &st) == -1)
		mkdir(path,0755);
}

int cache_app(struct app a)
{
	debug_print("cache_app: cache application_%llu_%04u\n",a.cluster_timestamp,a.id);
	FILE *outfile;
	char cache_file[256];

	sprintf(cache_file,"%s/applications/application_%llu_%04u\n",cache_path,a.cluster_timestamp,a.id);
	outfile = fopen (cache_file, "w");
	if (outfile == NULL)
	{
		fprintf(stderr, "\nError opend file\n");
		exit(1);
	}
	fwrite(&a, sizeof(struct app), 1, outfile);
	fclose(outfile);
	return 0;
}

int cache_cnt(struct cnt c)
{
	debug_print("cache_cnt: cache container_e%u_%llu_%04u_%02u_%06u\n",c.epoch,c.cluster_timestamp,c.app_id,c.attempt_id,c.id);
	FILE *outfile;
	char cache_file[512];

	sprintf(cache_file,"%s/containers/container_e%u_%llu_%04u_%02u_%06u\n",cache_path,c.epoch,c.cluster_timestamp,c.app_id,c.attempt_id,c.id);
	outfile = fopen (cache_file, "w");
	if (outfile == NULL)
	{
		fprintf(stderr, "\nError opend file\n");
		exit(1);
	}
	fwrite(&c, sizeof(struct cnt), 1, outfile);
	fclose(outfile);
	return 0;
}

int read_cached_cnt(unsigned int epoch, unsigned long long int cluster_timestamp, unsigned int app_id, unsigned int attempt_id, unsigned int container_id, struct cnt *c)
{
	FILE *infile;
	char cache_file[512];
	debug_print_verbose("read_cached_cnt: attempting to load container_e%u_%llu_%04u_%02u_%06u\n",epoch,cluster_timestamp,app_id,attempt_id,container_id);
	sprintf(cache_file,"%s/containers/container_e%u_%llu_%04u_%02u_%06u\n",cache_path,epoch,cluster_timestamp,app_id,attempt_id,container_id);
	infile = fopen (cache_file, "r");
	if (infile == NULL)
	{
		debug_print("read_cached_cnt: cache miss for container_e%u_%llu_%04u_%02u_%06u\n",epoch,cluster_timestamp,app_id,attempt_id,container_id);
		cnt_cache_miss++;
		return 0;
	}
	fread(c, sizeof(struct cnt), 1, infile);
	fclose(infile);
	debug_print_verbose("read_cached_cnt: cache hit for container_e%u_%llu_%04u_%02u_%06u\n",epoch,cluster_timestamp,app_id,attempt_id,container_id);
	cnt_cache_hit++;
	return 1;
}

int read_cached_app(unsigned long long int cluster_timestamp, unsigned int id, struct app *a)
{
	FILE *infile;
	char cache_file[256];
	debug_print_verbose("read_cached_app: attempting to load application_%llu_%04u\n",cluster_timestamp,id);
	sprintf(cache_file,"%s/applications/application_%llu_%04u\n",cache_path,cluster_timestamp,id);
	infile = fopen (cache_file, "r");
	if (infile == NULL)
	{
		debug_print("read_cached_app: cache miss for application_%llu_%04u\n",cluster_timestamp,id);
		app_cache_miss++;
		return 0;
	}
	fread(a, sizeof(struct app), 1, infile);
	fclose(infile);
	debug_print_verbose("read_cached_app: cache hit for application_%llu_%04u\n",cluster_timestamp,id);
	app_cache_hit++;
	return 1;
}

struct app_tree_node
{
	struct app *a;
	struct app_tree_node *left;
	struct app_tree_node *right;
	struct app_tree_node *up;
};

struct app_tree_node *app_tree_root = NULL;

struct app *get_app(unsigned long long int cluster_timestamp, unsigned int id)
{
	struct app_tree_node *node;
	node = app_tree_root;
	while(node != NULL)
	{
		if(node->a->cluster_timestamp == cluster_timestamp && node->a->id == id)
		{
			return node->a;
		}
		if(node->a->cluster_timestamp > cluster_timestamp)
		{
			node = node->left;
			continue;
		}
		if(node->a->cluster_timestamp < cluster_timestamp)
		{
			node = node->right;
			continue;
		}
		if(node->a->id > id)
		{
			node = node->left;
			continue;
		}
		if(node->a->id < id)
		{
			node = node->right;
			continue;
		}
		debug_print("get_app: Shouldn't be here\n");
	}
	return NULL;
}

int put_app(struct app *a)
{
	struct app *dst;
	struct app_tree_node *node;
	struct app_tree_node *last_node;


	// init tree on first run
	if(!app_tree_root)
	{
		app_tree_root = calloc(1,sizeof(struct app_tree_node));
		app_tree_root->a = calloc(1,sizeof(struct app));

		memcpy(app_tree_root->a,a, sizeof(struct app));

		app_tree_root->left = NULL;
		app_tree_root->right = NULL;
		app_tree_root->up = NULL;
		return 1;
	}

	// search for node
	node = app_tree_root;

	while(1)
	{
		if(node->a->cluster_timestamp == a->cluster_timestamp && node->a->id == a->id)
		{
			// app already exists in tree, do nothing
			return 1;
		}
		if(node->a->cluster_timestamp > a->cluster_timestamp)
		{
			// if left node exists, keep going
			if(node->left)
			{
				node = node->left;
				continue;
			}
			node->left = calloc(1,sizeof(struct app_tree_node));
			node->left->a = calloc(1,sizeof(struct app));
			memcpy(node->left->a, a, sizeof(struct app));
			return 1;
		}
		if(node->a->cluster_timestamp < a->cluster_timestamp)
		{
			if(node->right)
			{
				node = node->right;
				continue;
			}
			node->right = calloc(1,sizeof(struct app_tree_node));
			node->right->a = calloc(1,sizeof(struct app));
			memcpy(node->right->a, a, sizeof(struct app));
			return 1;
		}
		if(node->a->id > a->id)
		{
			if(node->left)
			{
				node = node->left;
				continue;
			}
			node->left = calloc(1,sizeof(struct app_tree_node));
			node->left->a = calloc(1,sizeof(struct app));
			memcpy(node->left->a, a, sizeof(struct app));
			return 1;
		}
		if(node->a->id < a->id)
		{
			if(node->right)
			{
				node = node->right;
				continue;
			}
			node->right = calloc(1,sizeof(struct app_tree_node));
			node->right->a = calloc(1,sizeof(struct app));
			memcpy(node->right->a, a, sizeof(struct app));
			return 1;
		}
		debug_print("put_app: Shoudn't be here\n");
	}
}

struct cnt_tree_node
{
	struct cnt *c;
	struct cnt_tree_node *left;
	struct cnt_tree_node *right;
	struct cnt_tree_node *up;
};

struct cnt_tree_node *cnt_tree_root = NULL;

struct cnt *get_cnt(unsigned int epoch, unsigned long long int cluster_timestamp, unsigned int app_id, unsigned int attempt_id, unsigned int id)
{
	struct cnt_tree_node *node;
	node = cnt_tree_root;
	while(node != NULL)
	{
		if(node->c->epoch == epoch && node->c->cluster_timestamp == cluster_timestamp && node->c->app_id == app_id && node->c->attempt_id == attempt_id && node->c->id == id)
		{
			return node->c;
		}
		else if(node->c->epoch >= epoch && node->c->cluster_timestamp >= cluster_timestamp && node->c->app_id >= app_id && node->c->attempt_id >= attempt_id && node->c->id >= id)
		{
			node = node->left;
			continue;
		}
		else
		{
			node = node->right;
			continue;
		}
	}
	return NULL;
}

void cnt_cpy(struct cnt *dst, struct cnt *src, int gc)
{
	debug_print("cnt_cpy: GC = %d\n",gc);
	dst->epoch = src->epoch;
	dst->cluster_timestamp = src->cluster_timestamp;
	dst->app_id = src->app_id;
	dst->attempt_id = src->attempt_id;
	dst->id = src->id;

	dst->mem_allocated = src->mem_allocated;
	dst->cores_allocated = src->cores_allocated;
	dst->started_time = src->started_time;

	dst->cpu_time = src->cpu_time;
	dst->rss = src->rss;

	if(gc)
	{
		debug_print("cnt_cpy: dst->gcm.pid = %u, src->gcm.pid = %u\n",dst->gcm.pid,src->gcm.pid);
		dst->gcm = src->gcm;
	}
}

int put_cnt(struct cnt *c, int gc)
{
	debug_print("put_cnt: GC = %d\n",gc);
	struct cnt *dst;
	struct cnt_tree_node *node;
	struct cnt_tree_node *last_node;
	unsigned long int c_1,c_2,c_3,node_1,node_2,node_3;

	c_1 = ((unsigned long int)(c->epoch))<<32;
	c_1 += c->app_id;
	c_2 = ((unsigned long int)(c->attempt_id))<<32;
	c_2 += c->id;
	c_3 = c->cluster_timestamp;

	// init tree on first run
	if(!cnt_tree_root)
	{
		cnt_tree_root = calloc(1,sizeof(struct cnt_tree_node));
		cnt_tree_root->c = calloc(1,sizeof(struct cnt));

		cnt_cpy(cnt_tree_root->c,c,gc);

		cnt_tree_root->left = NULL;
		cnt_tree_root->right = NULL;
		cnt_tree_root->up = NULL;
		return 1;
	}

	// search for node
	node = cnt_tree_root;

	while(1)
	{
		node_1 = ((unsigned long int)(node->c->epoch))<<32;
		node_1 += node->c->app_id;
		node_2 = ((unsigned long int)(node->c->attempt_id))<<32;
		node_2 += node->c->id;
		node_3 = node->c->cluster_timestamp;
		if(node_1 == c_1 && node_2 == c_2 && node_3 == c_3)
		{
			// If ID matches
			node->c->rss += c->rss;
			if(gc)
			{
				node->c->gcm = c->gcm;
			}
			return 1;
		}
		if(node_1 > c_1)
		{
			if(node->left)
			{
				node = node->left;
				continue;
			}
			node->left = calloc(1,sizeof(struct cnt_tree_node));
			node->left->c = calloc(1,sizeof(struct cnt));
			cnt_cpy(node->left->c,c,gc);
			return 1;
		}
		if(node_1 < c_1)
		{
			if(node->right)
			{
				node = node->right;
				continue;
			}
			node->right = calloc(1,sizeof(struct cnt_tree_node));
			node->right->c = calloc(1,sizeof(struct cnt));
			cnt_cpy(node->right->c,c,gc);
			return 1;
		}
		if(node_2 > c_2)
		{
			if(node->left)
			{
				node = node->left;
				continue;
			}
			node->left = calloc(1,sizeof(struct cnt_tree_node));
			node->left->c = calloc(1,sizeof(struct cnt));
			cnt_cpy(node->left->c,c,gc);
			return 1;
		}
		if(node_2 < c_2)
		{
			if(node->right)
			{
				node = node->right;
				continue;
			}
			node->right = calloc(1,sizeof(struct cnt_tree_node));
			node->right->c = calloc(1,sizeof(struct cnt));
			cnt_cpy(node->right->c,c,gc);
			return 1;
		}
		if(node_3 > c_3)
		{
			if(node->left)
			{
				node = node->left;
				continue;
			}
			node->left = calloc(1,sizeof(struct cnt_tree_node));
			node->left->c = calloc(1,sizeof(struct cnt));
			cnt_cpy(node->left->c,c,gc);
			return 1;
		}
		if(node_3 < c_3)
		{
			if(node->right)
			{
				node = node->right;
				continue;
			}
			node->right = calloc(1,sizeof(struct cnt_tree_node));
			node->right->c = calloc(1,sizeof(struct cnt));
			cnt_cpy(node->right->c,c,gc);
			return 1;
		}
		debug_print("put_cnt: Uh.... This shouldn't be reachable\n");
	}
}

int traverse_cnt(struct cnt_tree_node *node, void (*f)(), char *buff)
{
	if(node->left)
		traverse_cnt(node->left,f,buff);
	//printcnt(*(node->c));
	f(*(node->c),buff);
	if(node->right)
		traverse_cnt(node->right,f,buff);
}

void prune_cache()
{
	char prune_app_cache_path[256];
	char prune_cnt_cache_path[512];
	char cache_age[13];
	char* cmd[] = {"find",0,0,"-name","application_*","-type","f","-amin",0,"-exec","rm","{}",";",0};
	sprintf(prune_app_cache_path,"%s/applications",cache_path);
	sprintf(prune_cnt_cache_path,"%s/containers",cache_path);
	cmd[1] = prune_app_cache_path;
	cmd[2] = prune_cnt_cache_path;
	sprintf(cache_age,"%s%u","+",cache_expiry);
	cmd[8] = cache_age;
	debug_print("prune_cache: calling fork\n");
	pid_t pid = fork();

	if (pid == -1)
	{
		debug_print("prune_cache: fork failed\n");
		return;
	}
	else if (pid > 0)
	{
		int status;
		debug_print("prune_cache: parent: waiting\n");
		waitpid(pid, &status, 0);
		debug_print("prune_cache: parent: resume\n");
	}
	else
	{
		debug_print("prune_cache: child: pruning %s\n",cmd[1]);
		execvp(cmd[0],cmd);
		debug_print("prune_cache: child: unexpected return from execvp\n");
		_exit(EXIT_FAILURE);
	}
}

int getcnt_rm(unsigned int epoch, unsigned long long int cluster_timestamp, unsigned int app_id, unsigned int attempt_id, unsigned int container_id, struct cnt *c)
{
	// cnt_name,"%llu_%llu_%llu_%llu_%llu",&epoch,&cluster_timestamp,&app_id,&attempt_id,&container_id
	int failover=0,success=0;
	char cnt_url[512];
	char *ptr;
	char *ptr1;
	unsigned int mem_allocated_mb;
	debug_print("getcnt_rm: fetching container_e%u_%llu_%04u_%02u_%06u\n",epoch,cluster_timestamp,app_id,attempt_id,container_id);
	while(!success)
	{
		sprintf(cnt_url,"%s/ws/v1/cluster/apps/application_%llu_%04u/appattempts/appattempt_%llu_%04u_%06u/containers/container_e%u_%llu_%04u_%02u_%06u",
			active_rm,cluster_timestamp,app_id,cluster_timestamp,app_id,attempt_id,epoch,cluster_timestamp,app_id,attempt_id,container_id);
		debug_print_verbose("getcnt_rm: URL: %s\n",cnt_url);
		CURL *curl;
		CURLcode res;
		curl = curl_easy_init();
		if(!curl)
			return -1;
		struct string s;
		init_string(&s);
		curl_easy_setopt(curl, CURLOPT_URL, cnt_url);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &s);
		res = curl_easy_perform(curl);
		ptr = strstr(s.ptr,"is standby RM");
		if(!failover)
		{
			if(ptr)
			{
				failover = 1;
				debug_print("getcnt_rm: failover active RM from %s ",active_rm);
				active_rm = (char *)((unsigned long long int)rm1_url ^ (unsigned long long int) active_rm);
				active_rm = (char *)((unsigned long long int)rm2_url ^ (unsigned long long int) active_rm);
				debug_print("to %s\n",active_rm);
			}
			else
			{
				success = 1;
			}
		}
		else
		{
			success = 1;
			if(ptr)
				return -1;
		}

		if(success)
		{
			debug_print_verbose("getcnt_rm: response[%zu]: %s\n",s.len,s.ptr);
			c->epoch = epoch;
			c->cluster_timestamp = cluster_timestamp;
			c->app_id = app_id;
			c->attempt_id = attempt_id;
			c->id = container_id;
			if(strstr(s.ptr,"NotFoundException"))
			{
				c->cores_allocated = 0;
				c->mem_allocated = 0;
				c->started_time = 0;
				return -1;
			}

			ptr = strstr(s.ptr,"\"allocatedVCores\":");
			ptr1 = strstr(ptr,"\",");
			*(ptr1) = 0;
			sscanf(ptr+19,"%u",&c->cores_allocated);
			debug_print_verbose("getcnt_rm: %s\n",ptr);
			*(ptr1) = '}';

			ptr = strstr(s.ptr,"\"allocatedMB\":");
			ptr1 = strstr(ptr,"\",");
			*(ptr1) = 0;
			sscanf(ptr+15,"%u",&mem_allocated_mb);
			c->mem_allocated=((unsigned long long int)mem_allocated_mb*1024*1024);
			debug_print_verbose("getcnt_rm: %s\n",ptr);
			*(ptr1) = '"';

			ptr = strstr(s.ptr,"\"startedTime\":");
			ptr1 = strstr(ptr,"\",");
			*(ptr1) = 0;
			sscanf(ptr+15,"%llu",&c->started_time);
			debug_print_verbose("getcnt_rm: %s\n",ptr);
			*(ptr1) = '"';

		}

		free(s.ptr);

		curl_easy_cleanup(curl);
	}
}

int getapp_rm(unsigned long long int cluster_timestamp, unsigned int app_id, struct app *a)
{
	int failover=0,success=0;
	char app_url[256];
	char *ptr;
	char *ptr1;
	debug_print("getapp_rm: fetching application_%llu_%04u\n",cluster_timestamp,app_id);
	while(!success)
	{
		sprintf(app_url,"%s/ws/v1/cluster/apps/application_%llu_%04u",active_rm,cluster_timestamp,app_id);
		debug_print_verbose("getapp_rm: URL: %s\n",app_url);
		CURL *curl;
		CURLcode res;
		curl = curl_easy_init();
		if(!curl)
			return -1;
		struct string s;
		init_string(&s);
		curl_easy_setopt(curl, CURLOPT_URL, app_url);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &s);
		res = curl_easy_perform(curl);
		ptr = strstr(s.ptr,"is standby RM");
		if(!failover)
		{
			if(ptr)
			{
				failover = 1;
				debug_print("getapp_rm: failover active RM from %s ",active_rm);
				active_rm = (char *)((unsigned long long int)rm1_url ^ (unsigned long long int) active_rm);
				active_rm = (char *)((unsigned long long int)rm2_url ^ (unsigned long long int) active_rm);
				debug_print("to %s\n",active_rm);
			}
			else
			{
				success = 1;
			}
		}
		else
		{
			success = 1;
			if(ptr)
				return -1;
		}

		if(success)
		{
			if(strstr(s.ptr,"NotFoundException"))
			{
				strcpy(a->user,"<defunct>");
				strcpy(a->name,"<defunct>");
				strcpy(a->queue,"<defunct>");
				strcpy(a->type,"<defunct>");
				a->started_time = 0;
				return -1;
			}
			debug_print_verbose("getapp_rm: response[%zu]: %s\n",s.len,s.ptr);
			ptr = strstr(s.ptr,"\"user\":");
			ptr1 = strstr(ptr,",");
			*(ptr1-1) = 0;
			strncpy(a->user,ptr+8,63);
			*(ptr1-1) = '"';

			ptr = strstr(s.ptr,"\"name\":");
			ptr1 = strstr(ptr,",");
			*(ptr1-1) = 0;
			strncpy(a->name,ptr+8,127);
			*(ptr1-1) = '"';

			ptr = strstr(s.ptr,"\"queue\":");
			ptr1 = strstr(ptr,",");
			*(ptr1-1) = 0;
			strncpy(a->queue,ptr+9,127);
			*(ptr1-1) = '"';

			ptr = strstr(s.ptr,"\"applicationType\":");
			ptr1 = strstr(ptr,",");
			*(ptr1-1) = 0;
			strncpy(a->type,ptr+19,31);
			*(ptr1-1) = '"';

			ptr = strstr(s.ptr,"\"startedTime\":");
			ptr1 = strstr(ptr,",");
			//*(ptr1) = 0;
			sscanf(ptr+14,"%llu",&a->started_time);
			//*(ptr1) = ',';

			(*a).id = app_id;
			(*a).cluster_timestamp = cluster_timestamp;
		}

		free(s.ptr);

		curl_easy_cleanup(curl);
	}

	return 0;
}

unsigned long long int cnt_cpu_time(struct cnt c)
{
	char cnt_cpuacct_path[256];
	unsigned long long int cpuacct_usage;
	sprintf(cnt_cpuacct_path,"%s/container_e%u_%llu_%04u_%02u_%06u/cpuacct.usage",cgroup_root,c.epoch,c.cluster_timestamp,c.app_id,c.attempt_id,c.id);
	debug_print_verbose("cnt_cpu_time: reading %s\n",cnt_cpuacct_path);
	FILE* file = fopen (cnt_cpuacct_path, "r");
	if(!file)
	{
		debug_print("cnt_cpu_time: cpuacct not found: %s\n",cnt_cpuacct_path);
		return 0;
	}
	fscanf(file,"%llu",&cpuacct_usage);
	debug_print_verbose("cnt_cpu_time: cpuacct usage: %llu\n",cpuacct_usage);
	return cpuacct_usage;
}

void parsecgrp(char *cgrp, unsigned long long int rss, unsigned long long int pid)
{
	char cnt_name[128];
	char *ptr;
	unsigned int epoch,app_id,attempt_id,container_id;
	unsigned long long int cluster_timestamp;
	struct app a;
	struct cnt c = {};
	int gc = 0;
	ptr = strstr(cgrp,"/container");
	if(!ptr)
		return;
	ptr+=12;
	int i=0;
	while(*ptr != ',' && *ptr != 0)
	{
		cnt_name[i++] = *ptr;
		ptr++;
	}
	cnt_name[i] = 0;
	sscanf(cnt_name,"%u_%llu_%u_%u_%u",&epoch,&cluster_timestamp,&app_id,&attempt_id,&container_id);
	//printf("User: %s\n",a.user);
	if(!read_cached_app(cluster_timestamp,app_id,&a))
	{
		getapp_rm(cluster_timestamp,app_id,&a);
		cache_app(a);
	}
	if(!read_cached_cnt(epoch,cluster_timestamp,app_id,attempt_id,container_id,&c))
	{
		getcnt_rm(epoch,cluster_timestamp,app_id,attempt_id,container_id,&c);
		cache_cnt(c);
	}
	c.cpu_time = cnt_cpu_time(c);
	c.rss = rss;

	struct hsperf_file hsfile = {};
	memset(&(c.gcm),0,sizeof(struct gc_metrics));
	if(open_jstat(&hsfile, pid))
	{
		jstat(&(c.gcm), &hsfile);
		gc = 1;
	}
	close_jstat(&hsfile);

	//printapp(a);
	//printcnt(c);
	put_app(&a);
	put_cnt(&c,gc);
	return;
}

void gen()
{
	FILE *fp;
	char in[1024];
	char *cgroup_name;
	unsigned long long int rss,pid;
	// TODO: implement sort+uniq+sum(rss) using trie
	fp = popen("ps --no-header -u nobody -o pid,rss,cgroup", "r");
	if (fp == NULL)
	{
		exit(1);
	}
	while (fgets(in, sizeof(in), fp) != NULL)
	{
		debug_print_verbose("gen: read line from ps: %s\n", in);
		sscanf(in,"%llu\t%llu",&pid,&rss);
		rss*=1024;
		debug_print_verbose("gen: rss = %llu, pid = %llu\n", rss, pid);
		cgroup_name = strstr(in,"/hadoop-yarn");
		if(!cgroup_name)
			continue;
		debug_print_verbose("gen: calling parsecgrp(%s)\n",cgroup_name);
		parsecgrp(cgroup_name,rss,pid);
	}
	pclose(fp);
	traverse_cnt(cnt_tree_root,jsoncnt,kafka_buffer);
	puts(kafka_buffer);
	debug_print("gen: %u app cache hits, %u app cache misses. %.2f %% app cache hit rate\n",app_cache_hit,app_cache_miss,100*(float)app_cache_hit/((float)app_cache_miss+(float)app_cache_hit));
	debug_print("gen: %u container cache hits, %u container cache misses. %.2f %% container cache hit rate\n",cnt_cache_hit,cnt_cache_miss,100*(float)cnt_cache_hit/((float)cnt_cache_miss+(float)cnt_cache_hit));
	prune_cache();
}

int setopt(int argc, char *argv[])
{
	int opt;
	char *ptr;

	while ((opt = getopt (argc, argv, "r:c:e:k:t:")) != -1)
	{
		switch (opt)
		{
			case 'r':
				ptr = strstr(optarg,",");
				*ptr=0;
				strncpy(rm1_url,optarg,127);
				strncpy(rm2_url,ptr+1,127);
				*ptr=',';
				break;
			case 'c':
				strncpy(cache_path,optarg,255);
				break;
			case 'e':
				sscanf(optarg,"%u",&cache_expiry);
				break;
			case 'k':
				strncpy(kafka_brokers,optarg,255);
				break;
			case 't':
				strncpy(kafka_topic,optarg,31);
				break;
		}
	}
	return 0;
}

static void dr_msg_cb (rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque)
{
	run = 0;
	return;
}

int main(int argc, char *argv[])
{
	active_rm = rm1_url;
	setopt(argc, argv);
	initcache();
	gen();

	rd_kafka_conf_t *conf;
	conf = rd_kafka_conf_new();
	char errstr[512];
	rd_kafka_conf_set(conf, "bootstrap.servers", kafka_brokers, errstr, sizeof(errstr));
	debug_print("kafka: pushing to servers %s, topic %s\n",kafka_brokers,kafka_topic);
	rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
	rd_kafka_t *rk;
	rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
	size_t len = strlen(kafka_buffer);
	debug_print("kafka: pushing %lu bytes\n",len);
	rd_kafka_resp_err_t err;
	int i=0;
	do
	{
		debug_print("kafka: attempt %d\n",i);
		err = rd_kafka_producev(rk, RD_KAFKA_V_TOPIC(kafka_topic), RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
			RD_KAFKA_V_VALUE(kafka_buffer, len), RD_KAFKA_V_OPAQUE(NULL), RD_KAFKA_V_END);
		if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL)
		{
			debug_print("kafka: Queue full. Retrying.\n");
			rd_kafka_poll(rk, 1000);
		}
		rd_kafka_poll(rk, 0);
	} while(err);

	//while(run);

	return 0;
}
