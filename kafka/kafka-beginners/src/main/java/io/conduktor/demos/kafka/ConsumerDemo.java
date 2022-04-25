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


public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka consumer");

        String boot_strap_server = "172.26.32.246:9092";
        String topic = "demo_java";
        String group_id = "my_pd";


        // Create Consumer Property
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,boot_strap_server);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        // Create the Consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        // Subscribe consumer to our topics(s)
        consumer.subscribe(Arrays.asList(topic));

        // poll for new data
        while (true) {
            // poll(Duration.ofMillis(100)): gửi yêu cầu nhận dữ liệu và đợi trong 1000ms
            // trong 1000ms đó sẽ có hoặc không dữ liệu gửi qua
            // hết 1000ms qua vòng for bên dưới để tổng hợp dữ liệu

            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));
/*
            AUTO_OFFSET_RESET_CONFIG:   auto.commit.interval.ms = 5000ms
                                        enable.auto.commit = true

            Khi gọi poll lần đầu thì cứ mỗi 5s sau mới commit offset (cập nhật offset( bit trong chuỗi binary data))
 */
            for (ConsumerRecord<String,String> record : records){
                log.info("Key: " + record.key() + " value: "+ record.value());
                log.info("Partition: "+ record.partition() + " Offset: " + record.offset());
            }
        }
    }
}
