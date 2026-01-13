package com.example.app.topology;

import com.example.app.processor.PauseAwareProcessor;
import com.example.app.serde.BufferedEventsSerde;
import com.example.app.serde.EventSerde;
import com.example.app.serde.ResumeCommandSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class PauseTopologyBuilder {

        public void build(StreamsBuilder builder, PauseConfig config) {

                // 1. State Store with Explicit Logging (Durability)
                Map<String, String> changelogConfig = new HashMap<>();
                changelogConfig.put("min.insync.replicas", "2");

                var storeBuilder = Stores.keyValueStoreBuilder(
                                Stores.persistentKeyValueStore(config.bufferStoreName()),
                                Serdes.String(),
                                new BufferedEventsSerde())
                                .withLoggingEnabled(changelogConfig); // CRITICAL: Explicit durability

                builder.addStateStore(storeBuilder);

                // 2. Sources with Explicit Type Safety - Unified Stream
                // We read both topics, cast to Object, merge, and process.

                KStream<String, Object> mainAndTriggerStream = builder.stream(
                                config.mainTopic(),
                                Consumed.with(Serdes.String(), new EventSerde()))
                                .mapValues(v -> (Object) v)
                                .merge(
                                                builder.stream(
                                                                config.triggerTopic(),
                                                                Consumed.with(Serdes.String(),
                                                                                new ResumeCommandSerde()))
                                                                .mapValues(v -> (Object) v));

                // 3. Unified Processor
                mainAndTriggerStream.process(
                                () -> new PauseAwareProcessor(config.bufferStoreName(), config.statusStoreName()),
                                config.bufferStoreName() // Connect to buffer store
                )
                                .to(
                                                config.outputTopic(),
                                                Produced.with(Serdes.String(), new EventSerde()));
        }
}
