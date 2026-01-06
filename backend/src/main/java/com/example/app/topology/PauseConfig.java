package com.example.app.topology;

public record PauseConfig(
                String topicId,
                String mainTopic,
                String statusTopic,
                String triggerTopic,
                String outputTopic,
                String bufferStoreName) {
        public String statusStoreName() {
                return topicId + "-status-store";
        }
}
