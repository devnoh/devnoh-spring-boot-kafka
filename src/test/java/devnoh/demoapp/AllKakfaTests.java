package devnoh.demoapp;

import devnoh.demoapp.consumer.ReceiverTest;
import devnoh.demoapp.producer.SenderTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({SenderTest.class, ReceiverTest.class})
@Slf4j
public class AllKakfaTests {

    /*
    protected static final String SENDER_TOPIC = "sender.t";
    protected static final String RECEIVER_TOPIC = "helloworld.t";

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, SENDER_TOPIC);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        String kafkaBootstrapServers = embeddedKafka.getBrokersAsString();
        log.debug("kafkaServers='{}'", kafkaBootstrapServers);
        // override the property in application.properties
        System.setProperty("kafka.bootstrap.servers", kafkaBootstrapServers);
    }
    */
}
