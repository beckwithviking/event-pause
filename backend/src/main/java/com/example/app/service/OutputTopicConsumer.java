package com.example.app.service;

import com.example.app.model.Event;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

@Service
public class OutputTopicConsumer {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * Consumes recent messages from an output topic.
     * 
     * For production with multiple instances:
     * - Uses a shared consumer group ID so instances coordinate and share
     * partitions
     * - Does NOT commit offsets (read-only operation)
     * - Each instance only reads from its assigned partitions
     * 
     * Trade-offs:
     * - Shared group: Better for production, instances coordinate, but each
     * instance only sees
     * messages from its assigned partitions
     * - Unique group: Each instance sees all messages, but creates many consumer
     * groups
     * 
     * @param topicName   The topic to read from
     * @param maxMessages Maximum number of messages to return per partition
     * @return List of consumed messages
     */
    public List<Map<String, Object>> consumeRecentMessages(String topicName, int maxMessages) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Use a unique consumer group to avoid conflicts, though with assign() it
        // matters less
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "output-topic-reader-" + topicName + "-" + UUID.randomUUID());

        // For read-only operations, we don't want to commit offsets
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // Short session timeout for faster rebalancing (since we're short-lived)
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000);

        // Use latest as default, then we'll seek backwards to read recent messages
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Event.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxMessages);

        KafkaConsumer<String, Event> consumer = new KafkaConsumer<>(props);
        List<Map<String, Object>> messages = new ArrayList<>();

        try {
            // Manually assign all partitions to avoid group rebalancing delays
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topicName);
            List<org.apache.kafka.common.TopicPartition> topicPartitions = new ArrayList<>();

            if (partitionInfos != null) {
                for (PartitionInfo pi : partitionInfos) {
                    topicPartitions.add(new org.apache.kafka.common.TopicPartition(pi.topic(), pi.partition()));
                }
            }

            if (topicPartitions.isEmpty()) {
                System.out.println("[CONSUMER] No partitions found for topic " + topicName);
                return messages;
            }

            consumer.assign(topicPartitions);

            // No need to wait for assignment loop since assign() is immediate

            // Get end offsets for all partitions
            Map<org.apache.kafka.common.TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);

            // Seek to appropriate positions for each partition
            for (org.apache.kafka.common.TopicPartition partition : topicPartitions) {
                Long endOffset = endOffsets.get(partition);
                System.out.println("[CONSUMER] Topic=" + topicName + ", Partition=" + partition.partition()
                        + ", EndOffset=" + endOffset);
                if (endOffset != null && endOffset > 0) {
                    // Read from the last maxMessages, but don't go before 0
                    long startOffset = Math.max(0, endOffset - maxMessages);
                    System.out.println(
                            "[CONSUMER] Seeking to offset " + startOffset + " (will read up to " + endOffset + ")");
                    consumer.seek(partition, startOffset);
                } else {
                    // No messages in this partition
                    System.out.println("[CONSUMER] No messages in partition, seeking to end");
                    consumer.seekToEnd(Collections.singletonList(partition));
                }
            }

            // Poll loop to ensure we get all requested messages
            int attempts = 0;
            // Loop until we have enough messages or we try 5 times with no new data
            while (messages.size() < maxMessages && attempts < 5) {
                ConsumerRecords<String, Event> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    attempts++;
                    continue;
                } else {
                    attempts = 0; // Reset attempts if we got data
                }

                System.out.println("[CONSUMER] Polled " + records.count() + " messages from topic " + topicName);

                for (ConsumerRecord<String, Event> record : records) {
                    Map<String, Object> message = createMessageMap(record);
                    messages.add(message);
                }
            }

            if (messages.isEmpty()) {
                System.out
                        .println("[CONSUMER] No messages found in topic " + topicName + ". End offsets: " + endOffsets);
            }
        } catch (Exception e) {
            System.err.println("Error consuming messages: " + e.getMessage());
            e.printStackTrace();
        } finally {
            consumer.close();
        }

        return messages;
    }

    private Map<String, Object> createMessageMap(ConsumerRecord<String, Event> record) {
        Map<String, Object> message = new HashMap<>();
        message.put("key", record.key());
        message.put("eventId", record.value().id());
        message.put("data", record.value().data());
        message.put("timestamp", record.value().timestamp());
        message.put("partition", record.partition());
        message.put("offset", record.offset());
        message.put("consumedAt", Instant.now().toEpochMilli());
        return message;
    }
}
