package fr.mhaffar.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {
    private static final Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());

    public static void main(String[] args) {
        //System.out.println("Starting producer...");
        log.info("I'm a kafka Producer ...");

        //Create Producer Properties
        Properties props = new Properties();

        //Connect to localhost
        props.setProperty("bootstrap.servers", "localhost:9092");

        //Set producer properties
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());

        //Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //Create a Producer Record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("my-java-topic", "Hello World");

        //send data
        producer.send(record);

        //tell the producer to send all data and block until
        producer.flush();

        //flush and close the producer
        producer.close();
        // l done -- synchronous
    }
}
