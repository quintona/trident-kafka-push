package com.github.quintona;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import kafka.producer.KeyedMessage;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;

public class KafkaState implements State {
	
	ConcurrentLinkedQueue<KeyedMessage<String, String>> messages = new ConcurrentLinkedQueue<KeyedMessage<String, String>>();

	public static class Options implements Serializable {
		
		private static final long serialVersionUID = 1L;
		public String zookeeperConnectString = "127.0.0.1:2181";
		public String serializerClass = "kafka.serializer.StringEncoder";
		public String brokerList = "127.0.0.1:9092";
		
		public Options(){}

		public Options(String zookeeperConnectString, String serializerClass, String brokerList) {
			this.zookeeperConnectString = zookeeperConnectString;
			this.serializerClass = serializerClass;
			this.brokerList = brokerList;
		}
	}
	
	public static StateFactory transactional(Options options) {
        return new Factory(options, true);
    }
	
	public static StateFactory nonTransactional(String topic, Options options) {
        return new Factory(options, false);
    }

	protected static class Factory implements StateFactory {
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private Options options;
		boolean transactional;
		
		public Factory(Options options, boolean transactional){
			this.options = options;
			this.transactional = transactional;
		}

		@Override
		public State makeState(Map conf, IMetricsContext metrics,
				int partitionIndex, int numPartitions) {
			return new KafkaState(options, transactional);
		}

	}
	
	private Options options;
	Producer<String, String> producer;
	private boolean transactional;
	
	public KafkaState(Options options, boolean transactional){
		this.options = options;
		this.transactional = transactional;
		Properties props = new Properties();
		props.put("zk.connect", options.zookeeperConnectString);
		props.put("serializer.class", options.serializerClass);
		props.put("metadata.broker.list", options.brokerList);
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
	}

	@Override
	public void beginCommit(Long txid) {
		if(messages.size() > 0)
			throw new RuntimeException("Kafka State is invalid, the previous transaction didn't flush");
	}
	
	public void enqueue(KeyedMessage<String, String> message){
		if(transactional)
			messages.add(message);
		else
			sendMessage(message);
	}
	
	private void sendMessage(KeyedMessage<String, String> message){
		producer.send(message);
	}
	
	private void sendBatch(List<KeyedMessage<String, String>> messages){
		producer.send(messages);
	}

	@Override
	public void commit(Long txid) {
		KeyedMessage<String, String> message = messages.poll();
		List<KeyedMessage<String, String>> toSend = new ArrayList<KeyedMessage<String,String>>();
		while(message != null){
			toSend.add(message);
			message = messages.poll();
		}
		sendBatch(toSend);
	}

}
