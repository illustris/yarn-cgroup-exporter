#include <stdlib.h>
#include <string.h>
#include <curl/curl.h>
#include "yarn_rm_api.h"
#include "debug.h"

char* active_rm;

struct string
{
	char *ptr;
	size_t len;
};

// TODO: This will fail if rm 1 is down
void init_rm_api(char *rm)
{
	active_rm = rm;
}

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

int getcnt_rm(unsigned int epoch, unsigned long long int cluster_timestamp, unsigned int app_id, unsigned int attempt_id, unsigned int container_id, struct cnt *c, char *rm1_url, char *rm2_url)
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
	return 0;
}

int getapp_rm(unsigned long long int cluster_timestamp, unsigned int app_id, struct app *a, char *rm1_url, char *rm2_url)
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