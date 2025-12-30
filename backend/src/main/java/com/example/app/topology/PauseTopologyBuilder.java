package com.example.app.topology;

import com.example.app.processor.PauseAwareProcessor;
import com.example.app.processor.ResumeTriggerProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class PauseTopologyBuilder {

        @Autowired
        private SerdeProvider serdes;

        public void build(StreamsBuilder builder, PauseConfig config) {

                // 1. Define State Store for buffering paused events
                builder.addStateStore(
                                Stores.keyValueStoreBuilder(
                                                Stores.persistentKeyValueStore(config.bufferStoreName()),
                                                Serdes.String(),
                                                serdes.bufferedEventsSerde()));

                // 2. Main Processing Topology
                // Note: GlobalKTable stores are globally accessible, so we only need to connect
                // the buffer store
                builder.stream(config.mainTopic(), Consumed.with(Serdes.String(), serdes.eventSerde()))
                                .process(
                                                () -> new PauseAwareProcessor(config.bufferStoreName()),
                                                config.bufferStoreName()) // Connect to buffer store (GlobalKTable is
                                                                          // globally accessible)
                                .to(config.outputTopic(), Produced.with(Serdes.String(), serdes.eventSerde()));

                // 3. Resume Trigger Topology
                builder.stream(config.triggerTopic(), Consumed.with(Serdes.String(), serdes.resumeCommandSerde()))
                                .process(
                                                () -> new ResumeTriggerProcessor(config.bufferStoreName()),
                                                config.bufferStoreName())
                                .to(config.outputTopic(), Produced.with(Serdes.String(), serdes.eventSerde()));
        }
}
