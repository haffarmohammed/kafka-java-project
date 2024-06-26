package fr.mhaffar.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerWithShutdown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerWithShutdown.class.getSimpleName());

    public static void main(String[] args) {

        String groupId = "my-java-application";
        String topic = "my-java-topic";


        //System.out.println("Starting producer...");
        log.info("I'm a kafka Consumer ...");

        //Create Producer Properties
        Properties props = new Properties();

        //Connect to localhost
        props.setProperty("bootstrap.servers", "localhost:9092");

        //Set producer properties
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());
        props.setProperty("group.id", groupId);
        props.setProperty("auto.offset.reset", "earliest");

        //create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup() ...");
                consumer.wakeup();


                //join the main thread to allow the execution of the code in the main thread
                 try {
                     mainThread.join();
                 } catch (InterruptedException e) {
                     throw new RuntimeException(e);
                 }


            }
        });


        try {
            //subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            //poll for data
            while (true) {

                log.info("Consuming from topic: " + topic);

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key:" + record.key() + ", Value: " + record.value());
                    log.info("Partition:" + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shutdown");
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer ",e);
        } finally {
            consumer.close(); // close the consumer, this will also commit offset
            log.info("Consumer is now gracefully shutting down");
        }



    }
}
