all:
	gcc debug.c yarn_rm_api.c yarn_cgroup_exporter.c jstat.c -O3 -o yarn_exporter -lcurl -lrdkafka
debug:
	gcc -O0 -g debug.c yarn_rm_api.c yarn_cgroup_exporter.c jstat.c -o yarn_exporter -lcurl -lrdkafka
clean:
	rm yarn_exporter
