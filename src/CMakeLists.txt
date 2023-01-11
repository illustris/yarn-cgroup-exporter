project(HelloWorld)
cmake_minimum_required(VERSION 3.0)

find_package(CURL REQUIRED)

set(SOURCES debug.c kafka_client.c yarn_structs_cache.c yarn_rm_api.c yarn_cgroup_exporter.c jstat.c)
add_executable(yarn-cgroup-exporter ${SOURCES} ${HEADERS})
target_link_libraries(yarn-cgroup-exporter ${CURL_LIBRARY} rdkafka)

install (TARGETS yarn-cgroup-exporter DESTINATION bin)
