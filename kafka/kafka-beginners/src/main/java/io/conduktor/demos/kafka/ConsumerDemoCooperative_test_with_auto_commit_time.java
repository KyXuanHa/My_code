package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerDemoCooperative_test_with_auto_commit_time {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative_test_with_auto_commit_time.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka consumer");

        String boot_strap_server = "172.26.32.246:9092";
        String topic = "abc";
        String group_id = "my_third_application";

        long autoCommitMillisInterval = 500000;
        // Create Consumer Property
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,boot_strap_server);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, Long.toString(autoCommitMillisInterval));

        //properties.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,"");

        // Create the Consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        // Khi mà luồng hiện tại kết thúc khi nó vẫn đang chạy hoặc bị lỗi không thể tiếp tục
        // Nó sẽ xuất ra log.info và chạy consumer.wakeup() bên , try chạy mainThread lần nữa nhưng với consumer.wakeup()

            // Get current Thread:
            final Thread mainThread = Thread.currentThread();
            // Add the shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(){
                public void run(){
                    log.info("Detected a shutdown, let's exit by calling consumer.wakeup()");
                    consumer.wakeup();

                    try{
                        mainThread.join();
                    }catch (InterruptedException e){
                        e.printStackTrace();
                    }
            }
        });

        try{
            // Subscribe consumer to our topics(s)
            consumer.subscribe(Arrays.asList(topic));

            // poll for new data
            while (true) {
                // poll(Duration.ofMillis(100)): chờ 1000ms từ lần cuối nhận được, nếu không nhận được gì trả về tập rỗng
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String,String> record : records){
                    log.info("Key: " + record.key() + " value: "+ record.value());
                    log.info("Partition: "+ record.partition() + " Offset: " + record.offset());
                }
            }
        }catch (WakeupException e){
            log.info("Wake up exception!");
        }catch (Exception e){
            log.error("Unexpected exception");
        }finally {
            consumer.close();
            log.info("The consumer is now gracefully closed!");
        }


    }
}
