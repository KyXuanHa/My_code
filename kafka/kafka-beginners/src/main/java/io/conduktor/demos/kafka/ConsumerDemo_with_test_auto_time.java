package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerDemo_with_test_auto_time {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo_with_test_auto_time.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka consumer");

        String boot_strap_server = "172.26.32.246:9092";
        String topic = "demo_java";
        String group_id = "my_pd";

        // Thoi gian de commit offset - Vi no' ma` code loi~
        // Cach sua hay nhat la them consumer.wakeup() giong nhu ConsumerDemowithShutdown
        long autoCommitMillisInterval = 50000000;

        // Create Consumer Property
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,boot_strap_server);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        // Thoi gian de commit offset
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, Long.toString(autoCommitMillisInterval));

        // Create the Consumer  - Vi no' ma` code loi~
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        // Subscribe consumer to our topics(s)
        consumer.subscribe(Arrays.asList(topic));

        // poll for new data
        while (true) {
            // poll(Duration.ofMillis(100)): g???i y??u c???u nh???n d??? li???u v?? ?????i trong 1000ms
            // trong 1000ms ???? s??? c?? ho???c kh??ng d??? li???u g???i qua
            // h???t 1000ms qua v??ng for b??n d?????i ????? t???ng h???p d??? li???u

            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));
/*
            AUTO_OFFSET_RESET_CONFIG:   auto.commit.interval.ms = 5000ms
                                        enable.auto.commit = true

            Khi g???i poll l???n ?????u th?? c??? m???i 5s sau m???i commit offset (c???p nh???t offset( bit trong chu???i binary data))
 */
            for (ConsumerRecord<String,String> record : records){
                log.info("Key: " + record.key() + " value: "+ record.value());
                log.info("Partition: "+ record.partition() + " Offset: " + record.offset());
            }
        }
    }
}
