package com.cloudcomputing.samza.nycabs.application;

import com.cloudcomputing.samza.nycabs.DriverMatchConfig;
import com.cloudcomputing.samza.nycabs.DriverMatchTask;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.task.StreamTaskFactory;

public class DriverMatchTaskApplication implements TaskApplication {
    // Consider modify this zookeeper address, localhost may not be a good choice.
    // If this task application is executing in slave machine.
    /** TODO: Fill in Master node IP Addresses */
    private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("172.31.29.230:2181"); // TODO: fill in

    // Consider modify the bootstrap servers address. This example only cover one
    // address.
    /** TODO: Fill in Bootstrap Servers IP Addresses */
    private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("172.31.22.177:9092","172.31.29.25:9092","172.31.29.230:9092"); // TODO: fill in
    private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

    @Override
    public void describe(TaskApplicationDescriptor taskApplicationDescriptor) {
        // Define a system descriptor for Kafka.
        KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor("kafka")
                .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
                .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
                .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

        // Hint about streams, please refer to DriverMatchConfig.java
        // We need two input streams "events", "driver-locations", one output stream
        // "match-stream".

        // Define your input and output descriptor in here.
        // Reference solution:
        // https://github.com/apache/samza-hello-samza/blob/master/src/main/java/samza/examples/wikipedia/task/application/WikipediaStatsTaskApplication.java
        KafkaInputDescriptor driverLocInputDescriptor = kafkaSystemDescriptor
                .getInputDescriptor("driver-locations", new JsonSerde<>());

        KafkaInputDescriptor eventsInputDescriptor = kafkaSystemDescriptor
                .getInputDescriptor("events", new JsonSerde<>());

        KafkaOutputDescriptor matchStreamOutputDescriptor = kafkaSystemDescriptor
                .getOutputDescriptor("match-stream", new JsonSerde<>());
        
        // Bound you descriptor with your taskApplicationDescriptor in here.
        // Please refer to the same link.
        taskApplicationDescriptor.withDefaultSystem(kafkaSystemDescriptor);
        taskApplicationDescriptor.withInputStream(driverLocInputDescriptor);
        taskApplicationDescriptor.withInputStream(eventsInputDescriptor);
        taskApplicationDescriptor.withOutputStream(matchStreamOutputDescriptor);

        taskApplicationDescriptor.withTaskFactory((StreamTaskFactory) () -> new DriverMatchTask());
    }
}
