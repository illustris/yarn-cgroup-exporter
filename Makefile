all:
	gcc yarn_cgroup_exporter.c -O3 -o yarn_exporter -lcurl -lrdkafka
debug:
	gcc -O0 -g yarn_cgroup_exporter.c -o yarn_exporter -lcurl -lrdkafka
clean:
	rm yarn_exporter
