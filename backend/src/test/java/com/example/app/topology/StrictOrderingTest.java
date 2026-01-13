package com.example.app.topology;

import com.example.app.model.Event;
import com.example.app.model.KeyStatus;
import com.example.app.model.ResumeCommand;
import com.example.app.serde.EventSerde;
import com.example.app.serde.KeyStatusSerde;
import com.example.app.serde.ResumeCommandSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class StrictOrderingTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Event> inputTopic;
    private TestInputTopic<String, ResumeCommand> resumeTopic;
    private TestInputTopic<String, KeyStatus> statusTopic;
    private TestOutputTopic<String, Event> outputTopic;
    private KeyValueStore<String, List<Event>> bufferStore;

    private static final String MAIN_TOPIC = "demo-in";
    private static final String RESUME_TOPIC = "demo-resume";
    private static final String STATUS_TOPIC = "demo-status";
    private static final String OUTPUT_TOPIC = "demo-out";
    private static final String BUFFER_STORE = "demo-buffer-store";
    private static final String STATUS_STORE = "demo-status-store";

    @BeforeEach
    void setup() {
        PauseTopologyBuilder topologyBuilder = new PauseTopologyBuilder();
        PauseConfig config = new PauseConfig("demo", MAIN_TOPIC, STATUS_TOPIC, RESUME_TOPIC, OUTPUT_TOPIC,
                BUFFER_STORE);

        StreamsBuilder builder = new StreamsBuilder();
        // Mimic StreamsOrchestrator setup for GlobalKTable
        builder.globalTable(STATUS_TOPIC,
                org.apache.kafka.streams.kstream.Consumed.with(Serdes.String(), new KeyStatusSerde()),
                org.apache.kafka.streams.kstream.Materialized.as(STATUS_STORE));

        topologyBuilder.build(builder, config);
        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, EventSerde.class.getName());

        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic(MAIN_TOPIC, Serdes.String().serializer(),
                new EventSerde().serializer());
        resumeTopic = testDriver.createInputTopic(RESUME_TOPIC, Serdes.String().serializer(),
                new ResumeCommandSerde().serializer());
        statusTopic = testDriver.createInputTopic(STATUS_TOPIC, Serdes.String().serializer(),
                new KeyStatusSerde().serializer());
        outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, Serdes.String().deserializer(),
                new EventSerde().deserializer());

        bufferStore = testDriver.getKeyValueStore(BUFFER_STORE);
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void testWorstCaseScenario_StrictOrdering() {
        String key = "123";

        System.out.println("=== START TEST: Strict Ordering Worst Case Scenario ===");
        System.out.println("Step 1: Initial State (Active)");

        // 1. Initial State: Process 3 events immediately (Active)
        // Status defaults to ACTIVE if not set, or we can set it explicitly.
        statusTopic.pipeInput(key, KeyStatus.ACTIVE);

        inputTopic.pipeInput(key, new Event("e1", "data1", System.currentTimeMillis()));
        inputTopic.pipeInput(key, new Event("e2", "data2", System.currentTimeMillis()));
        inputTopic.pipeInput(key, new Event("e3", "data3", System.currentTimeMillis()));

        System.out.println("-> Sent 3 initial events.");
        assertEquals(3, outputTopic.getQueueSize());
        assertEquals("e1", outputTopic.readRecord().value().id());
        assertEquals("e2", outputTopic.readRecord().value().id());
        assertEquals("e3", outputTopic.readRecord().value().id());
        System.out.println("-> Verified 3 initial events processed immediately.");

        // 2. Pause State: Pause key 123
        System.out.println("\nStep 2: Pause Key " + key);
        statusTopic.pipeInput(key, KeyStatus.PAUSED);

        // Send 500 events
        System.out.println("-> Sending 500 events while PAUSED...");
        for (int i = 0; i < 500; i++) {
            inputTopic.pipeInput(key, new Event("b" + i, "buffered-" + i, System.currentTimeMillis()));
        }

        // Verify NO output and BUFFER full
        assertEquals(0, outputTopic.getQueueSize());
        List<Event> buffered = bufferStore.get(key);
        assertNotNull(buffered);
        assertEquals(500, buffered.size());
        System.out.println("-> Checked Output Queue: Size=0 (Correct)");
        System.out.println("-> Checked Buffer Store: Size=500 (Correct)");

        // 3. Resume Race Condition
        System.out.println("\nStep 3: Resume Race Condition Simualtion");
        System.out.println("-> Setting Status to ACTIVE");
        // Update Status to ACTIVE
        statusTopic.pipeInput(key, KeyStatus.ACTIVE);

        System.out.println("-> Injecting ResumeCommand and 2 New Events (n1, n2) simultaneously...");
        // "Simultaneous" arrival: Resume Command AND 2 New Events
        resumeTopic.pipeInput(key, new ResumeCommand("RESUME"));
        inputTopic.pipeInput(key, new Event("n1", "new-1", System.currentTimeMillis()));
        inputTopic.pipeInput(key, new Event("n2", "new-2", System.currentTimeMillis()));

        // Verification
        // Total expected: 500 buffered + 2 new = 502 events
        // Order MUST be: b0...b499, n1, n2
        int outputSize = (int) outputTopic.getQueueSize();
        System.out.println("-> Total Output Queue Size: " + outputSize);
        assertEquals(502, outputSize);

        List<Event> results = outputTopic.readValuesToList();

        // Verify Start
        System.out.println("-> Verifying ordering...");
        System.out.println("   First Event: " + results.get(0).id() + " (Expected: b0)");
        assertEquals("b0", results.get(0).id());

        // Verify End of Buffer
        System.out.println("   Last Buffered Event: " + results.get(499).id() + " (Expected: b499)");
        assertEquals("b499", results.get(499).id());

        // Verify New Events come AFTER buffer
        System.out.println("   First New Event: " + results.get(500).id() + " (Expected: n1)");
        assertEquals("n1", results.get(500).id());

        System.out.println("   Second New Event: " + results.get(501).id() + " (Expected: n2)");
        assertEquals("n2", results.get(501).id());

        System.out.println("\n=== SUCCESS: Strict Ordering Verified! ===");
    }
}
