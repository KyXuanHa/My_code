package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemowithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemowithCallback.class.getSimpleName());

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
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"1");

        // Create te Producer
            //Tao 1 produce bang "KafkaProducer<String,String>" 2 cai <String,String> cua "key" and "value"
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        for (int i=0;i<100;i++){
            // Producer record
            //Day la Message: <key,value> gui den kafkacluster
            String topic = "abc";
//            String key = "id_"+(i%3);
            String key = "id_";
//            String value = "Message thứ" +i;
            String value = "Message thứ"+i ;
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic,key,value);
            // send Data - asynchronous
            producer.send(producerRecord, new Callback() {
                @Override
                // Thông báo chuyển đến mỗi khi producer gửi thành công 1 producerRecord
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    //thuc thi moi khi gui thanh cong
                    if (e==null){
                        log.info("Received new data \n " +
                                "Topic: " + metadata.topic() +"\n "+
                                "Key: " + producerRecord.key() + "\n " +    // metadata phản hồi không chứa key. key thuộc về producer
                                "Partition: " + metadata.partition() +"\n "+
                                "Offset: " + metadata.offset() +"\n "+
                                "Timestamp: " + metadata.timestamp());
                    }else {
                        log.error("Error while sending from PRODUCER");
                    }
                }
            });

//            // Nghi 1s de cho producer co the phan hoi den tat ca partition, neu ko chi co 1 partition nhan
//            try {
//                Thread.sleep(1000);
//            }catch (InterruptedException e){
//                e.printStackTrace();
//            }

        }

        // flush and close Producer
        producer.flush();
        producer.close();
    }
}
