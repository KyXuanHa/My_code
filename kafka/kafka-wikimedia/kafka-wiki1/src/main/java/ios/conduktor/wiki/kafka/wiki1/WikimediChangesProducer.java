package ios.conduktor.wiki.kafka.wiki1;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.EventHandler;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediChangesProducer {

    public static  void main(String[] args) throws InterruptedException {

        String bootstrap_server = "";

        Properties properties =new Properties();
        //properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"172.26.46.41:9092");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");


//        // set safe producer config(kafa<=2.8)
//        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
//        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
//        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

       // String topic = "wikimedia.recentchange";
        String topic = "kk";

        // event_source lấy data từ URL theo 1 chuỗi các thao tác(ntn thì ko biết)
        // kết thúc mỗi thao tác sẽ sinh ra events
        // Với mỗi xự kiện diễn ra sẽ xử lý theo eventHandler

        EventHandler eventHandler = new WikimediChamgesHandler(producer,topic);
        String url =  "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler,URI.create(url));
        EventSource eventSource = builder.build();
        // star kafka producer in another thread
        eventSource.start();

        // Nghi o day 10'
        TimeUnit.MINUTES.sleep(10);

    }
}
