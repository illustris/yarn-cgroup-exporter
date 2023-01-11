#include "yarn_structs.h"
void init_rm_api(char *rm);
int getcnt_rm(unsigned int epoch, unsigned long long int cluster_timestamp, unsigned int app_id, unsigned int attempt_id, unsigned int container_id, struct cnt *c, char *rm1_url, char *rm2_url);
int getapp_rm(unsigned long long int cluster_timestamp, unsigned int app_id, struct app *a, char *rm1_url, char *rm2_url);