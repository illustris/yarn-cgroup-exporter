#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "debug.h"
#include "jstat.h"

int open_jstat(struct hsperf_file *hsfile, char *hsperf_basepath, unsigned int pid)
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
		key_length = (key_length < 64 ? key_length:63);
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