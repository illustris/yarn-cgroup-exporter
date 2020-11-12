#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <time.h>

#include "jstat.h"
#include "debug.h"
#include "yarn_rm_api.h"
#include "kafka_client.h"
#include "yarn_structs_cache.h"

//Container_e{epoch}_{clusterTimestamp}_{appId}_{attemptId}_{containerId}
//Container_{clusterTimestamp}_{appId}_{attemptId}_{containerId}

char rm1_url[128];
char rm2_url[128];

unsigned int init_cache_expiry = 60;

char init_cache_path[256] = "/dev/shm/yarn_exporter_cache";
char cgroup_root[64] = "/sys/fs/cgroup/cpu/hadoop-yarn";

char hsperf_basepath[] = "/tmp/hsperfdata_nobody";

char kafka_buffer[128*1024] = "";
char kafka_brokers[256];
char kafka_topic[32];

unsigned long int timestamp;
char hostname[128];

int read_cached_app(unsigned long long int, unsigned int, struct app*);

void jsoncnt(struct cnt c,char *json, unsigned long int t, char *h)
{
	struct app a;
	char buf[2048];
	char *buf_ptr;
	buf_ptr = buf;
	int ret = read_cached_app(c.cluster_timestamp,c.app_id,&a);

	buf_ptr+=sprintf(buf_ptr,"{\"timestamp\":%lu000,\"hostname\":\"%s\",\"application\":\"application_%llu_%04u\",\"user\":\"%s\",\"name\":\"%s\",\"queue\":\"%s\",\"app_start_time\":%llu,\"type\":\"%s\",",
		t,h,a.cluster_timestamp,a.id,a.user,a.name,a.queue,a.started_time,a.type);
	buf_ptr+=sprintf(buf_ptr,"\"container\":\"container_e%u_%llu_%04u_%02u_%06u\",",c.epoch,c.cluster_timestamp,c.app_id,c.attempt_id,c.id);
	buf_ptr+=sprintf(buf_ptr,"\"epoch\":%u,\"cluster_timestamp\":%llu,\"app_id\":%u,\"attempt_id\":%u,\"container_id\":%u,\"mem_allocated\":%llu,\"cores_allocated\":%u,\"container_start_time\":%llu,\"cpu_time\":%llu,\"rss\":%llu,",
	c.epoch,c.cluster_timestamp,c.app_id,c.attempt_id,c.id,c.mem_allocated,c.cores_allocated,c.started_time,c.cpu_time/1000000,c.rss);
	buf_ptr+=sprintf(buf_ptr,"\"current_heap_capacity\":%lu,\"current_heap_usage\":%lu,\"young_gc_cnt\":%lu,\"final_gc_cnt\":%lu,\"pid\":%u,\"young_gc_time\":%f,\"final_gc_time\":%f,\"total_gc_time\":%f}",
	c.gcm.current_heap_capacity,c.gcm.current_heap_usage,c.gcm.young_gc_cnt,c.gcm.final_gc_cnt,c.gcm.pid,c.gcm.young_gc_time,c.gcm.final_gc_time,c.gcm.total_gc_time);

	strcat(json,buf);
	return;
}

struct cnt_tree_node
{
	struct cnt *c;
	struct cnt_tree_node *left;
	struct cnt_tree_node *right;
	struct cnt_tree_node *up;
};

struct cnt_tree_node *cnt_tree_root = NULL;

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
	//debug_print("gen: %u app cache hits, %u app cache misses. %.2f %% app cache hit rate\n",app_cache_hit,app_cache_miss,100*(float)app_cache_hit/((float)app_cache_miss+(float)app_cache_hit));
	//debug_print("gen: %u container cache hits, %u container cache misses. %.2f %% container cache hit rate\n",cnt_cache_hit,cnt_cache_miss,100*(float)cnt_cache_hit/((float)cnt_cache_miss+(float)cnt_cache_hit));
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
				strncpy(init_cache_path,optarg,255);
				break;
			case 'e':
				sscanf(optarg,"%u",&init_cache_expiry);
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

int main(int argc, char *argv[])
{
	timestamp = time(NULL);

	gethostname(hostname,127);

	init_rm_api(rm1_url);

	setopt(argc, argv);
	init_cache(init_cache_path, init_cache_expiry);
	gen();

	init_kafka(kafka_brokers);
	push_kafka(kafka_buffer,kafka_topic);

	return 0;
}
