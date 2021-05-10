package work.lollipops.kafka.tutorial;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author zhaohaoren
 */
public class PureKafkaDemo {

    private static final String DEFAULT_BROKERS = "localhost:9092";
    private static final String DEFAULT_TOPIC = "topic-demo";

    private static final String GROUP_ID = "group.demo";


    private static void producer() {
        Properties properties = new Properties();
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // ☆ 推荐使用这种方式配置参数
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put("bootstrap.servers", DEFAULT_BROKERS);
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(DEFAULT_TOPIC, "Hello Kafka!");
        try {
            System.out.println("生产了：" + record.value());
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }

    private static void customer() throws InterruptedException {
        Properties properties = new Properties();
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("bootstrap.servers", DEFAULT_BROKERS);
        // 设置消费组
        properties.put("group.id", GROUP_ID);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // 订阅topic
        consumer.subscribe(Collections.singletonList(DEFAULT_TOPIC));
        // 循环消费
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            Thread.sleep(10000);
            records.forEach(record -> System.out.println("消费到：" + record.value()));
        }
    }


    public static void main(String[] args) throws InterruptedException {
        System.out.println("-- 开始生产 --");
        producer();
        System.out.println("-- 开始消费 --");
        customer();
    }
}
