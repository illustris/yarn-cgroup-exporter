void init_cache(char *base_path, unsigned int init_cache_expiry);
int cache_cnt(struct cnt c);
int read_cached_cnt(unsigned int epoch, unsigned long long int cluster_timestamp, unsigned int app_id, unsigned int attempt_id, unsigned int container_id, struct cnt *c);
int cache_app(struct app a);
int read_cached_app(unsigned long long int cluster_timestamp, unsigned int id, struct app *a);
int prune_cache();