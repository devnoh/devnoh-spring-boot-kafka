package devnoh.demoapp.consumer;

import devnoh.demoapp.kafka.consumer.Receiver;
import lombok.extern.slf4j.Slf4j;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class ReceiverTest {

    protected static final String RECEIVER_TOPIC = "helloworld.t";

    @Autowired
    private Receiver receiver;

    @Autowired
    KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        String kafkaBootstrapServers = embeddedKafka.getBrokersAsString();
        log.info("kafkaServers='{}'", kafkaBootstrapServers);
        // override the property in application.properties
        System.setProperty("kafka.bootstrap.servers", kafkaBootstrapServers);
    }

    @Test
    public void testReceive() throws Exception {

        // set up the Kafka producer properties
        Map<String, Object> senderProperties =
                KafkaTestUtils.senderProps(embeddedKafka.getBrokersAsString());

        // create a Kafka producer factory
        ProducerFactory<String, String> producerFactory =
                new DefaultKafkaProducerFactory<String, String>(senderProperties);

        // create a Kafka template
        KafkaTemplate<String, String> template = new KafkaTemplate<>(producerFactory);
        // set the default topic to send to
        template.setDefaultTopic(RECEIVER_TOPIC);

        // get the ConcurrentMessageListenerContainers
        for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry.getListenerContainers()) {
            if (messageListenerContainer instanceof ConcurrentMessageListenerContainer) {
                ConcurrentMessageListenerContainer<String, String> concurrentMessageListenerContainer =
                        (ConcurrentMessageListenerContainer<String, String>) messageListenerContainer;

                // as the topic is created implicitly, the default number of partitions is 1
                int partitionsPerTopic = 1;
                // wait until the container has the required number of assigned partitions
                ContainerTestUtils.waitForAssignment(concurrentMessageListenerContainer, partitionsPerTopic);
            }
        }

        // send the message
        template.sendDefault("Hello Spring Kafka Receiver!");
        //template.send(RECEIVER_TOPIC, "Hello Spring Kafka Receiver!");

        receiver.getLatch().await(10000, TimeUnit.MILLISECONDS);
        assertThat(receiver.getLatch().getCount()).isEqualTo(0);
    }
}
