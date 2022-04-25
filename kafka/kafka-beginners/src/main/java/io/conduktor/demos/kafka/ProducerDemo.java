package io.conduktor.demos.kafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;


public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka producer");

        // Create Producer Property

        Properties properties = new Properties();
            // Tao 1 properties tu "java.util.Properties" va set Property: "bootstrap.servers","key","value
        /*
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.setProperty("key.serializer",StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());
        */
        // pr
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"172.26.43.182:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Create te Producer
            //Tao 1 produce bang "KafkaProducer<String,String>" 2 cai <String,String> cua "key" and "value"
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        // Producer record
            //Day la Message: <key,value> gui den kafkacluster
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_java","kaka");
        // send Data - asynchronous
        producer.send(producerRecord);

        // flush and close Producer
        producer.flush();
        producer.close();
    }
}
