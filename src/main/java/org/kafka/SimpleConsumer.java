package org.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class SimpleConsumer {
    public static void main(String[] args) throws Exception {
    	
    	// Check arguments
        if(args.length == 0){
	         System.err.println("Enter topic name");
	         return;
        }
        
        // topic
        String topicName = args[0].toString();
        
	    //Kafka consumer configuration
	    Properties props = new Properties();
        props.put("zookeeper.connect", "localhost:2181");
        props.put("zookeeper.session.timeout.ms", "3000");
        props.put("zookeeper.sync.time.ms", "1000");
	    props.put("group.id", "test");
	    props.put("auto.commit.interval.ms", "1000");
	    ConsumerConfig config = new ConsumerConfig(props);
	    ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);

	    //
	    System.out.println("begin pull ...");
	    Map<String, Integer> topicMap = new HashMap<String, Integer>();
	    topicMap.put(topicName, 1);
	    Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamsMap = consumer.createMessageStreams(topicMap);
	    List<KafkaStream<byte[], byte[]>> streamList = consumerStreamsMap.get(topicName);
	    for(final KafkaStream<byte[], byte[]> stream: streamList) {
	    	ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
	    	while(consumerIte.hasNext()) {
	    		System.out.println("Message from single topic :: " 
	    	        + new String(consumerIte.next().message()));
	    	}
	    }
	    
	    if(consumer!=null) {
	    	consumer.shutdown();
	    }
	    
	    System.out.println("pull stopped");
	}
}