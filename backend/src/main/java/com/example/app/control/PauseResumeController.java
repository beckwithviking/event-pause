package com.example.app.control;

import com.example.app.model.Event;
import com.example.app.model.KeyStatus;
import com.example.app.model.ResumeCommand;
import com.example.app.service.OutputTopicConsumer;
import com.example.app.service.StoreQueryService;
import com.example.app.topology.PauseConfig;
import com.example.app.topology.StreamsOrchestrator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/control")
public class PauseResumeController {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final StreamsOrchestrator orchestrator;
    private final OutputTopicConsumer topicConsumer;
    private final StoreQueryService storeQueryService;

    @Autowired
    public PauseResumeController(KafkaTemplate<String, Object> kafkaTemplate,
            StreamsOrchestrator orchestrator,
            OutputTopicConsumer topicConsumer,
            StoreQueryService storeQueryService) {
        this.kafkaTemplate = kafkaTemplate;
        this.orchestrator = orchestrator;
        this.topicConsumer = topicConsumer;
        this.storeQueryService = storeQueryService;
    }

    // Pause
    @PostMapping("/{topicId}/pause/{key}")
    public ResponseEntity<Void> pause(@PathVariable String topicId, @PathVariable String key) {
        PauseConfig config = orchestrator.getConfig(topicId);
        if (config == null)
            return ResponseEntity.notFound().build();
        // Use flow-specific status topic
        kafkaTemplate.send(config.statusTopic(), key, KeyStatus.PAUSED);
        return ResponseEntity.accepted().build();
    }

    // Resume
    @PostMapping("/{topicId}/resume/{key}")
    public ResponseEntity<Void> resume(@PathVariable String topicId, @PathVariable String key) {
        PauseConfig config = orchestrator.getConfig(topicId);
        if (config == null)
            return ResponseEntity.notFound().build();

        // 1. Set status to ACTIVE
        // Use flow-specific status topic
        kafkaTemplate.send(config.statusTopic(), key, KeyStatus.ACTIVE);

        // 2. Trigger buffer drain via resume topic
        // The resume topic is monitored by ResumeTriggerProcessor which triggers the
        // drain logic
        kafkaTemplate.send(config.triggerTopic(), key, ResumeCommand.resume());

        return ResponseEntity.accepted().build();
    }

    // Send Test Event
    @PostMapping("/{topicId}/send")
    public ResponseEntity<Map<String, String>> sendEvent(
            @PathVariable String topicId,
            @RequestParam String key,
            @RequestParam String data) {
        PauseConfig config = orchestrator.getConfig(topicId);
        if (config == null)
            return ResponseEntity.notFound().build();
        Event event = new Event(UUID.randomUUID().toString(), data, System.currentTimeMillis());
        kafkaTemplate.send(config.mainTopic(), key, event);
        Map<String, String> response = new HashMap<>();
        response.put("message", "Event sent: " + event.id());
        response.put("eventId", event.id());
        return ResponseEntity.ok(response);
    }

    // --- Monitoring Endpoints ---

    @GetMapping("/{topicId}/input-messages")
    public ResponseEntity<List<Map<String, Object>>> getInputMessages(
            @PathVariable String topicId,
            @RequestParam(defaultValue = "50") int limit) {
        PauseConfig config = orchestrator.getConfig(topicId);
        if (config == null)
            return ResponseEntity.notFound().build();
        return ResponseEntity.ok(topicConsumer.consumeRecentMessages(config.mainTopic(), limit));
    }

    @GetMapping("/{topicId}/output-messages")
    public ResponseEntity<List<Map<String, Object>>> getOutputMessages(
            @PathVariable String topicId,
            @RequestParam(defaultValue = "50") int limit) {
        PauseConfig config = orchestrator.getConfig(topicId);
        if (config == null)
            return ResponseEntity.notFound().build();
        return ResponseEntity.ok(topicConsumer.consumeRecentMessages(config.outputTopic(), limit));
    }

    @GetMapping("/{topicId}/buffer/{key}")
    public ResponseEntity<List<Event>> getBufferedEvents(
            @PathVariable String topicId,
            @PathVariable String key) {
        PauseConfig config = orchestrator.getConfig(topicId);
        if (config == null)
            return ResponseEntity.notFound().build();
        return ResponseEntity.ok(storeQueryService.getBufferedEvents(config.bufferStoreName(), key));
    }

    @GetMapping("/{topicId}/status/{key}")
    public ResponseEntity<Map<String, Object>> getKeyStatus(
            @PathVariable String topicId,
            @PathVariable String key) {
        PauseConfig config = orchestrator.getConfig(topicId);
        if (config == null)
            return ResponseEntity.notFound().build();
        KeyStatus status = storeQueryService.getKeyStatus(config.statusStoreName(), key);
        Map<String, Object> result = new HashMap<>();
        result.put("key", key);
        result.put("status", status);
        return ResponseEntity.ok(result);
    }
}
