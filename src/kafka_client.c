#include <string.h>
#include <librdkafka/rdkafka.h>
#include "debug.h"
#include "kafka_client.h"

rd_kafka_conf_t *conf;
rd_kafka_t *rk;

static void dr_msg_cb (rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque)
{
	return;
}

void init_kafka(char *kafka_brokers)
{
	char errstr[512];
	conf = rd_kafka_conf_new();
	rd_kafka_conf_set(conf, "bootstrap.servers", kafka_brokers, errstr, sizeof(errstr));
	//debug_print("kafka: pushing to servers %s, topic %s\n",kafka_brokers,kafka_topic);
	rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
	rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
}

void push_kafka(char *buffer, char *topic)
{
	size_t len = strlen(buffer);
	debug_print("kafka: pushing %lu bytes\n",len);
	rd_kafka_resp_err_t err;
	int i=0;
	do
	{
		debug_print("kafka: attempt %d\n",i);
		err = rd_kafka_producev(rk, RD_KAFKA_V_TOPIC(topic), RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
			RD_KAFKA_V_VALUE(buffer, len), RD_KAFKA_V_OPAQUE(NULL), RD_KAFKA_V_END);
		if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL)
		{
			debug_print("kafka: Queue full. Retrying.\n");
			rd_kafka_poll(rk, 1000);
		}
		rd_kafka_poll(rk, 0);
	} while(err);
	rd_kafka_flush(rk, 10*1000);
}