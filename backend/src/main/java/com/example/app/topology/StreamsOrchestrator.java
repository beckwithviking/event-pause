package com.example.app.topology;

import com.example.app.config.PauseFlowProperties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.util.List;
import java.util.Map;

@Configuration
@EnableKafkaStreams
public class StreamsOrchestrator {

    @Autowired
    private PauseTopologyBuilder topologyBuilder;

    @Autowired
    private SerdeProvider serdes;

    private final Map<String, PauseConfig> configMap;

    @Autowired
    public StreamsOrchestrator(PauseFlowProperties properties) {
        // Initialize config map from properties
        this.configMap = properties.getFlows().stream()
                .collect(java.util.stream.Collectors.toMap(PauseConfig::topicId, config -> config));
    }

    public List<PauseConfig> getTopicConfigs() {
        return List.copyOf(configMap.values());
    }

    public PauseConfig getConfig(String topicId) {
        return configMap.get(topicId);
    }

    @Bean
    public Topology combinedTopology(StreamsBuilder builder) {
        for (PauseConfig config : getTopicConfigs()) {
            // 1. Set up the GlobalKTable for this flow's status topic
            // We use a unique store name per flow to avoid conflicts
            builder.globalTable(
                    config.statusTopic(),
                    Consumed.with(Serdes.String(), serdes.keyStatusSerde()),
                    Materialized.as(config.statusStoreName()));

            // 2. Build individual sub-topology
            topologyBuilder.build(builder, config);
        }

        // 3. Return the fully combined topology
        return builder.build();
    }
}
