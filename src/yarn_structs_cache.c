#include <sys/stat.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/types.h>
//#include <sys/types.h>
#include "debug.h"
#include "yarn_structs.h"
#include "yarn_structs_cache.h"

char cache_path[256];

struct stat st;

// TODO: access cache stats from outside
unsigned int app_cache_hit;
unsigned int app_cache_miss;
unsigned int cnt_cache_hit;
unsigned int cnt_cache_miss;

unsigned int cache_expiry;

void init_cache(char *base_path, unsigned int init_cache_expiry)
{
	app_cache_hit = 0;
	app_cache_miss = 0;
	cnt_cache_hit = 0;
	cnt_cache_miss = 0;
	strncpy(cache_path,base_path,255);
	cache_expiry = init_cache_expiry;
	char path[288];
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
	char cache_file[288];

	sprintf(cache_file,"%s/applications/application_%llu_%04u",cache_path,a.cluster_timestamp,a.id);
	outfile = fopen (cache_file, "w");
	if (outfile == NULL)
	{
		fprintf(stderr, "\nError opend file\n");
		return 0;
	}
	fwrite(&a, sizeof(struct app), 1, outfile);
	fclose(outfile);
	return 1;
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
		return 0;
	}
	fwrite(&c, sizeof(struct cnt), 1, outfile);
	fclose(outfile);
	return 1;
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
	char cache_file[288];
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

int prune_cache()
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
		return 0;
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
		return 0;
	}
	return 1;
}