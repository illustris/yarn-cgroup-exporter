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

#define VERBOSE 0
#define debug_print_verbose(fmt, ...) \
	do { if (VERBOSE) fprintf(stderr, fmt, ##__VA_ARGS__); fflush(stderr); } while (0)

char rm1_url[128];
char rm2_url[128];
char* active_rm;

char cache_path[256] = "/dev/shm/yarn_exporter_cache";
unsigned int cache_hit = 0;
unsigned int cache_miss = 0;

unsigned int cache_expiry = 2;

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

void printapp(struct app a)
{
	printf("struct app\n{\n\tunsigned long long int cluster_timestamp = %llu;\n\tunsigned int app_id = %u;\n\tchar user[64] = %s;\n\tchar name[128] = %s;\n\tchar queue[128] = %s;\n\tunsigned long long int started_time = %llu;\n\tchar type[32] = %s;\n}\n\n",
	a.cluster_timestamp,a.id,a.user,a.name,a.queue,a.started_time,a.type);
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
		cache_miss++;
		return 0;
	}
	fread(a, sizeof(struct app), 1, infile);
	fclose(infile);
	debug_print_verbose("read_cached_app: cache hit for application_%llu_%04u\n",cluster_timestamp,id);
	cache_hit++;
	return 1;
}

void prune_cache()
{
	char prune_cache_path[256];
	char cache_age[12];
	char* cmd[] = {"find",0,"-name","application_*","-type","f","-amin",0,"-exec","rm","{}",";",0};
	sprintf(prune_cache_path,"%s/applications",cache_path);
	cmd[1] = prune_cache_path;
	sprintf(cache_age,"%s%u","+",cache_expiry);
	cmd[7] = cache_age;
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

void parsecgrp(char *cgrp)
{
	char cnt_name[128];
	char *ptr;
	unsigned long long int epoch,cluster_timestamp,app_id,attempt_id,container_id;
	struct app a;
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
	sscanf(cnt_name,"%llu_%llu_%llu_%llu_%llu",&epoch,&cluster_timestamp,&app_id,&attempt_id,&container_id);
	//printf("User: %s\n",a.user);
	if(!read_cached_app(cluster_timestamp,app_id,&a))
	{
		getapp_rm(cluster_timestamp,app_id,&a);
		cache_app(a);
	}
	printapp(a);
	return;
}

void gen()
{
	FILE *fp;
	char cgrp[1024];
	fp = popen("/bin/ps --no-header -u nobody -o cgroup", "r");
	if (fp == NULL)
	{
		exit(1);
	}
	while (fgets(cgrp, sizeof(cgrp), fp) != NULL)
	{
		//printf("%s", cgrp);
		debug_print_verbose("gen: calling parsecgrp(%s)\n",cgrp);
		parsecgrp(cgrp);
	}
	pclose(fp);
	debug_print("gen: %u cache hits, %u cache misses. %.2f %% cache hit rate\n",cache_hit,cache_miss,100*(float)cache_hit/((float)cache_miss+(float)cache_hit));
	debug_print("Calling prune_cache");
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
