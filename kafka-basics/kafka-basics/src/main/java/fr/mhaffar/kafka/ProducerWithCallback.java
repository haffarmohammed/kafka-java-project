package fr.mhaffar.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerWithCallback.class.getSimpleName());

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

        props.setProperty("batch.size", "400");

        // props.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        //Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);




        // Create a Producer Record && send it to the topic
        for (int j = 0; j < 10; j++) {
            for ( int i = 0; i < 30; i++ ) {

                //Create a Producer Record
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("my-java-topic", "Hello World" + i);

                //send data
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // executes every time a record successfully sent or an exception is thrown
                        if (e == null) {
                            // the record was successfully sent
                            log.info("Received new metadata \n"+
                                    "Topic:" + recordMetadata.topic() + "\n" +
                                    "Partition:" + recordMetadata.partition() + "\n" +
                                    "Offset:" + recordMetadata.offset() + "\n" +
                                    "Timestamp:" + recordMetadata.timestamp());
                        } else {
                            log.error("Error while sending message", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }




        //tell the producer to send all data and block until
        producer.flush();

        //flush and close the producer
        producer.close();
        // l done -- synchronous
    }
}
