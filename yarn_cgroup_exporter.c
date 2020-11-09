#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <curl/curl.h>

//Container_e{epoch}_{clusterTimestamp}_{appId}_{attemptId}_{containerId}
//Container_{clusterTimestamp}_{appId}_{attemptId}_{containerId}

#define DEBUG 1
#define debug_print(fmt, ...) \
	do { if (DEBUG) fprintf(stderr, fmt, ##__VA_ARGS__); fflush(stderr); } while (0)

#define VERBOSE 1
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

	unsigned int mem_allocated;
	unsigned int cores_allocated;
	unsigned long long int started_time;

	unsigned long long int cpu_time;
	unsigned long long int rss;
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
	printf("struct cnt\n{\n\tunsigned int epoch = %u;\n\tunsigned long long int cluster_timestamp = %llu;\n\tunsigned int app_id = %u;\n\tunsigned int attempt_id = %u;\n\tunsigned int id = %u;\n\tunsigned int mem_allocated = %u;\n\tunsigned int cores_allocated = %u;\n\tunsigned long long int started_time = %llu;\n\tcpu_time = %llu\n\tunsigned long long int rss = %llu\n}\n\n",
	c.epoch,c.cluster_timestamp,c.app_id,c.attempt_id,c.id,c.mem_allocated,c.cores_allocated,c.started_time,c.cpu_time,c.rss);
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

			ptr = strstr(s.ptr,"\"allocatedVCores\":");
			ptr1 = strstr(ptr,"\",");
			*(ptr1) = 0;
			sscanf(ptr+19,"%u",&c->cores_allocated);
			debug_print_verbose("getcnt_rm: %s\n",ptr);
			*(ptr1) = '}';

			ptr = strstr(s.ptr,"\"allocatedMB\":");
			ptr1 = strstr(ptr,"\",");
			*(ptr1) = 0;
			sscanf(ptr+15,"%u",&c->mem_allocated);
			debug_print_verbose("getcnt_rm: %s\n",ptr);
			*(ptr1) = '"';

			ptr = strstr(s.ptr,"\"startedTime\":");
			ptr1 = strstr(ptr,"\",");
			*(ptr1) = 0;
			sscanf(ptr+15,"%llu",&c->started_time);
			debug_print_verbose("getcnt_rm: %s\n",ptr);
			*(ptr1) = '"';

			// unsigned int epoch, unsigned long long int cluster_timestamp, unsigned int app_id, unsigned int attempt_id, unsigned int container_id, struct cnt *c
			(*c).epoch = epoch;
			(*c).cluster_timestamp = cluster_timestamp;
			(*c).app_id = app_id;
			(*c).attempt_id = attempt_id;
			(*c).id = container_id;
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

void parsecgrp(char *cgrp, unsigned long long int rss)
{
	char cnt_name[128];
	char *ptr;
	unsigned int epoch,app_id,attempt_id,container_id;
	unsigned long long int cluster_timestamp;
	struct app a;
	struct cnt c;
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
	printapp(a);
	printcnt(c);
	return;
}

void gen()
{
	FILE *fp;
	char in[1024];
	char *cgroup_name;
	unsigned long long int rss;
	// TODO: implement sort+uniq+sum(rss) using trie
	fp = popen("ps --no-header -u nobody -o rss,cgroup", "r");
	if (fp == NULL)
	{
		exit(1);
	}
	while (fgets(in, sizeof(in), fp) != NULL)
	{
		debug_print_verbose("gen: read line from ps: %s\n", in);
		sscanf(in,"%llu",&rss);
		debug_print_verbose("gen: rss = %u\n", rss);
		cgroup_name = strstr(in,"/hadoop-yarn");
		if(!cgroup_name)
			continue;
		debug_print_verbose("gen: calling parsecgrp(%s)\n",cgroup_name);
		parsecgrp(cgroup_name,rss);
	}
	pclose(fp);
	debug_print("gen: %u app cache hits, %u app cache misses. %.2f %% app cache hit rate\n",app_cache_hit,app_cache_miss,100*(float)app_cache_hit/((float)app_cache_miss+(float)app_cache_hit));
	debug_print("gen: %u container cache hits, %u container cache misses. %.2f %% container cache hit rate\n",cnt_cache_hit,cnt_cache_miss,100*(float)cnt_cache_hit/((float)cnt_cache_miss+(float)cnt_cache_hit));
	prune_cache();
}

int setopt(int argc, char *argv[])
{
	int opt;
	char *ptr;

	while ((opt = getopt (argc, argv, "r:c:e:")) != -1)
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
		}
	}
	return 0;
}

int main(int argc, char *argv[])
{
	active_rm = rm1_url;
	setopt(argc, argv);
	initcache();
	gen();
	return 0;
}
