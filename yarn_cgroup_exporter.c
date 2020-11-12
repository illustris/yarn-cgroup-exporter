#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <librdkafka/rdkafka.h>
#include <time.h>

#include "jstat.h"
#include "debug.h"
#include "yarn_rm_api.h"

//Container_e{epoch}_{clusterTimestamp}_{appId}_{attemptId}_{containerId}
//Container_{clusterTimestamp}_{appId}_{attemptId}_{containerId}

char rm1_url[128];
char rm2_url[128];

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

unsigned long int timestamp;
char hostname[128];

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

int read_cached_app(unsigned long long int, unsigned int, struct app*);

void jsoncnt(struct cnt c,char *json, unsigned long int t, char *h)
{
	struct app a;
	char buf[2048];
	char *buf_ptr;
	buf_ptr = buf;
	int ret = read_cached_app(c.cluster_timestamp,c.app_id,&a);

	buf_ptr+=sprintf(buf_ptr,"{\"timestamp\":%lu000,\"hostname\":\"%s\",\"application_id\":\"application_%llu_%04u\",\"user\":\"%s\",\"name\":\"%s\",\"queue\":\"%s\",\"app_start_time\":%llu,\"type\":\"%s\",",
		t,h,a.cluster_timestamp,a.id,a.user,a.name,a.queue,a.started_time,a.type);
	buf_ptr+=sprintf(buf_ptr,"\"container\":\"container_e%u_%llu_%04u_%02u_%06u\",",c.epoch,c.cluster_timestamp,c.app_id,c.attempt_id,c.id);
	buf_ptr+=sprintf(buf_ptr,"\"epoch\":%u,\"cluster_timestamp\":%llu,\"app_id\":%u,\"attempt_id\":%u,\"id\":%u,\"mem_allocated\":%llu,\"cores_allocated\":%u,\"started_time\":%llu,\"cpu_time\":%llu,\"rss\":%llu,",
	c.epoch,c.cluster_timestamp,c.app_id,c.attempt_id,c.id,c.mem_allocated,c.cores_allocated,c.started_time,c.cpu_time,c.rss);
	buf_ptr+=sprintf(buf_ptr,"\"current_heap_capacity\":%lu,\"current_heap_usage\":%lu,\"young_gc_cnt\":%lu,\"final_gc_cnt\":%lu,\"pid\":%u,\"young_gc_time\":%f,\"final_gc_time\":%f,\"total_gc_time\":%f}",
	c.gcm.current_heap_capacity,c.gcm.current_heap_usage,c.gcm.young_gc_cnt,c.gcm.final_gc_cnt,c.gcm.pid,c.gcm.young_gc_time,c.gcm.final_gc_time,c.gcm.total_gc_time);

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

	sprintf(cache_file,"%s/applications/application_%llu_%04u",cache_path,a.cluster_timestamp,a.id);
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

	sprintf(cache_file,"%s/containers/container_e%u_%llu_%04u_%02u_%06u",cache_path,c.epoch,c.cluster_timestamp,c.app_id,c.attempt_id,c.id);
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
	sprintf(cache_file,"%s/containers/container_e%u_%llu_%04u_%02u_%06u",cache_path,epoch,cluster_timestamp,app_id,attempt_id,container_id);
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
	sprintf(cache_file,"%s/applications/application_%llu_%04u",cache_path,cluster_timestamp,id);
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

int traverse_cnt(struct cnt_tree_node *node, void (*f)(), char *buff, unsigned int t, char *h)
{
	if(node->left)
		traverse_cnt(node->left,f,buff,t,h);
	//printcnt(*(node->c));
	f(*(node->c),buff,t,h);
	if(node->right)
		traverse_cnt(node->right,f,buff,t,h);
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
		getapp_rm(cluster_timestamp,app_id,&a, rm1_url, rm2_url);
		cache_app(a);
	}
	if(!read_cached_cnt(epoch,cluster_timestamp,app_id,attempt_id,container_id,&c))
	{
		getcnt_rm(epoch,cluster_timestamp,app_id,attempt_id,container_id,&c, rm1_url, rm2_url);
		cache_cnt(c);
	}
	c.cpu_time = cnt_cpu_time(c);
	c.rss = rss;

	struct hsperf_file hsfile = {};
	memset(&(c.gcm),0,sizeof(struct gc_metrics));
	if(open_jstat(&hsfile, hsperf_basepath, pid))
	{
		jstat(&(c.gcm), &hsfile);
		gc = 1;
	}
	close_jstat(&hsfile);

	//printapp(a);
	//printcnt(c);
	//put_app(&a);
	put_cnt(&c,gc);
	return;
}

void gen()
{
	FILE *fp;
	char in[1024];
	char *cgroup_name;
	unsigned long long int rss,pid;
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
	traverse_cnt(cnt_tree_root,jsoncnt,kafka_buffer,timestamp,hostname);
	//puts(kafka_buffer);
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
	return;
}

int main(int argc, char *argv[])
{
	timestamp = time(NULL);

	gethostname(hostname,127);

	init_rm_api(rm1_url);

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
	rd_kafka_flush(rk, 10*1000);

	return 0;
}
