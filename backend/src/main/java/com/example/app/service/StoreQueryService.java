package com.example.app.service;

import com.example.app.model.Event;
import com.example.app.model.KeyStatus;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

@Service
public class StoreQueryService {

    @Autowired
    private StreamsBuilderFactoryBean factoryBean;

    public List<Event> getBufferedEvents(String storeName, String key) {
        KafkaStreams streams = factoryBean.getKafkaStreams();
        if (streams == null || streams.state().equals(KafkaStreams.State.NOT_RUNNING)) {
            return Collections.emptyList();
        }

        try {
            ReadOnlyKeyValueStore<String, List<Event>> store = streams.store(
                    StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
            List<Event> events = store.get(key);
            return events != null ? events : Collections.emptyList();
        } catch (InvalidStateStoreException e) {
            // Store might not be ready yet
            return Collections.emptyList();
        }
    }

    public KeyStatus getKeyStatus(String key) {
        KafkaStreams streams = factoryBean.getKafkaStreams();
        if (streams == null) {
            return KeyStatus.ACTIVE; // Default
        }

        try {
            // GlobalKTable store name
            ReadOnlyKeyValueStore<String, Object> store = streams.store(
                    StoreQueryParameters.fromNameAndType("key-status-store", QueryableStoreTypes.keyValueStore()));

            Object value = store.get(key);
            if (value instanceof org.apache.kafka.streams.state.ValueAndTimestamp) {
                return ((org.apache.kafka.streams.state.ValueAndTimestamp<KeyStatus>) value).value();
            } else if (value instanceof KeyStatus) {
                return (KeyStatus) value;
            }
            return KeyStatus.ACTIVE;
        } catch (InvalidStateStoreException e) {
            return KeyStatus.ACTIVE;
        } catch (Exception e) {
            System.err.println("Error querying status store: " + e.getMessage());
            return KeyStatus.ACTIVE;
        }
    }
}
