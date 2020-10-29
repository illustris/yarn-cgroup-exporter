#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <curl/curl.h>

//Container_e{epoch}_{clusterTimestamp}_{appId}_{attemptId}_{containerId}
//Container_{clusterTimestamp}_{appId}_{attemptId}_{containerId}

char rm1_url[128];
char rm2_url[128];
char* active_rm;

struct string {
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

// "user", "name", "queue", "startedTime", "applicationType"
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
	if (stat("/dev/shm/yarn_exporter_cache", &st) == -1)
		mkdir("/dev/shm/yarn_exporter_cache",0700);
	if (stat("/dev/shm/yarn_exporter_cache/applications", &st) == -1)
		mkdir("/dev/shm/yarn_exporter_cache/applications",0700);
	if (stat("/dev/shm/yarn_exporter_cache/containers", &st) == -1)
		mkdir("/dev/shm/yarn_exporter_cache/containers",0700);
}

int getapp_rm(unsigned long long int cluster_timestamp, unsigned int app_id, struct app *a)
{
	int failover=0,success=0;
	char app_url[256];
	char *ptr;
	char *ptr1;
	while(!success)
	{
		printf("IN LOOP\n");
		sprintf(app_url,"%s/ws/v1/cluster/apps/application_%llu_%04u",active_rm,cluster_timestamp,app_id);
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
				active_rm = (char *)((unsigned long long int)rm1_url ^ (unsigned long long int) active_rm);
				active_rm = (char *)((unsigned long long int)rm2_url ^ (unsigned long long int) active_rm);
				printf("RM failover \n");
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
		//printf("%lu\n%s\n", s.len, s.ptr);
		// Need to save "user", "name", "queue", "startedTime", "applicationType"

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
	printf("%s\n",cnt_name);
	printf("%llu_%llu_%04llu_%02llu_%06llu\n\n",epoch,cluster_timestamp,app_id,attempt_id,container_id);
	getapp_rm(cluster_timestamp,app_id,&a);
	//printf("User: %s\n",a.user);
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
		parsecgrp(cgrp);
	}
	pclose(fp);
}

int setopt(int argc, char *argv[])
{
	int opt;
	char *ptr;
	while ((opt = getopt (argc, argv, "r:p:")) != -1)
	{
		switch (opt)
		{
			case 'r':
				ptr = strstr(optarg,",");
				*ptr=0;
				strncpy(rm1_url,optarg,128);
				strncpy(rm2_url,ptr+1,128);
				*ptr=',';
				break;
			//case 'o':
			//	printf ("Output file: \"%s\"\n", optarg);
			//	break;
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
