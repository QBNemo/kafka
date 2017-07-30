package org.kafka;

import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class SimpleProducer {
 
    public static void main(String[] args) throws Exception{
    
        // Check arguments
        if(args.length == 0){
            System.err.println("Enter topic name");
            return;
        }
    
        //Assign topicName to string variable
        String topicName = args[0].toString();
    
        // Producer configuration
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");   
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
    
        // send message
        System.out.println("Sending messages...");
        for(int i = 0; i < 10; i++) {
            String now = new Date().toLocaleString();
            String message = "Message publishing time - " + now;
            System.out.println(message);
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(topicName, message);
            producer.send(data);
            Thread.sleep(3000);
        }
    
        producer.close();
        System.out.println("Sending stopped");
    }
}
