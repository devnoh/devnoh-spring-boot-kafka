package devnoh.demoapp.producer;

import devnoh.demoapp.AllKafkaTests;
import devnoh.demoapp.kafka.producer.Sender;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.kafka.test.assertj.KafkaConditions.value;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class SenderTest {

    @Autowired
    Sender sender;

    /*
    public static final String SENDER_TOPIC = "sender.t";

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, SENDER_TOPIC);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        String kafkaBootstrapServers = embeddedKafka.getBrokersAsString();
        log.info("kafkaServers='{}'", kafkaBootstrapServers);
        // override the property in application.properties
        System.setProperty("kafka.bootstrap.servers", kafkaBootstrapServers);
    }
    */

    @Test
    public void testSend() throws Exception {

        // set up the Kafka consumer properties
        Map<String, Object> consumerProperties =
                KafkaTestUtils.consumerProps("sender_group", "false", AllKafkaTests.embeddedKafka);

        // create a Kafka consumer factory
        DefaultKafkaConsumerFactory<String, String> consumerFactory =
                new DefaultKafkaConsumerFactory<>(consumerProperties);
        // set the topic that needs to be consumed
        ContainerProperties containerProperties = new ContainerProperties(AllKafkaTests.SENDER_TOPIC);

        // create a Kafka MessageListenerContainer
        KafkaMessageListenerContainer<String, String> container =
                new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);

        // create a thread safe queue to store the received message
        BlockingQueue<ConsumerRecord<String, String>> records = new LinkedBlockingQueue<>();
        // setup a Kafka message listener
        container.setupMessageListener(new MessageListener<String, String>() {
            @Override
            public void onMessage(ConsumerRecord<String, String> record) {
                log.info(record.toString());
                records.add(record);
            }
        });

        // start the container and underlying message listener
        container.start();
        // wait until the container has the required number of assigned partitions
        ContainerTestUtils.waitForAssignment(container, AllKafkaTests.embeddedKafka.getPartitionsPerTopic());

        // send the message
        String greeting = "Hello Spring Kafka Sender!";
        sender.send(AllKafkaTests.SENDER_TOPIC, greeting);
        // check that the message was received
        assertThat(records.poll(10, TimeUnit.SECONDS)).has(value(greeting));

        // stop the container
        container.stop();
    }

}
