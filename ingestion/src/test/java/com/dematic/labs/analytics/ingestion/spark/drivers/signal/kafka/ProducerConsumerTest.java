package com.dematic.labs.analytics.ingestion.spark.drivers.signal.kafka;

import com.dematic.labs.toolkit.helpers.bigdata.communication.Signal;
import com.dematic.labs.toolkit.helpers.bigdata.communication.SignalUtils;
import com.google.common.base.Strings;
import info.batey.kafka.unit.KafkaUnitRule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

@Ignore //todo: need to fix, broken after kafka 10 upgrade
public final class ProducerConsumerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerConsumerTest.class);
    // starts an embedded kafka and zookeeper
    @Rule
    public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule(findRandomOpenPortOnAllLocalInterfaces(),
            findRandomOpenPortOnAllLocalInterfaces());

    @Test
    public void producerConsumer() throws TimeoutException, IOException {
        // 1) create a kafka topic
        final String signalTopic = "SignalTopic";
        kafkaUnitRule.getKafkaUnit().createTopic(signalTopic);
        // 2) send the signals
        sendSignals(5, signalTopic, kafkaUnitRule.getKafkaPort());
        // 3) read and assert
        final List<String> messages = kafkaUnitRule.getKafkaUnit().readMessages(signalTopic, 5);
        Assert.assertEquals(5, messages.size());
    }

    private static void sendSignals(final int num, final String topic, final int port) {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:" + port);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        final int[] count = {1};
        IntStream.range(0, num).forEach(request -> {
                    String signalJson = null;
                    try {
                        signalJson = SignalUtils.signalToJson(createSignal(count[0]));
                    } catch (final Throwable any) {
                        LOGGER.error("Unexpected Error: converting to json", any);
                    }
                    if (!Strings.isNullOrEmpty(signalJson)) {
                        final ProducerRecord<String, String> keyedMessage =
                                new ProducerRecord<>(topic, null, signalJson);
                        try {

                            producer.send(keyedMessage);
                        } catch (Throwable t) {
                            t.printStackTrace();
                        }
                    }
                    count[0]++;
                }
        );
    }

    private static Signal createSignal(final int number) {
        final Long longValue = (long) number;
        return new Signal(null, longValue, longValue, "2016-06-14T00:00:00Z",
                SignalUtils.toTimestampFromInstance(Instant.now()), longValue, longValue, longValue, "test",
                Collections.emptyList());
    }

    private static Integer findRandomOpenPortOnAllLocalInterfaces() {
        try {
            try (final ServerSocket socket = new ServerSocket(0)) {
                return socket.getLocalPort();
            }
        } catch (final Throwable wrap) {
            throw new IllegalStateException(wrap);
        }
    }
}
