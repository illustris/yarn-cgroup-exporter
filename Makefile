all:
	gcc yarn_cgroup_exporter.c -o yarn_exporter -lcurl
clean:
	rm yarn_exporter
