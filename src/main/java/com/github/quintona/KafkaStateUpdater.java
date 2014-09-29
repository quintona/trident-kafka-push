package com.github.quintona;

import java.util.List;

import kafka.producer.KeyedMessage;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

public class KafkaStateUpdater extends BaseStateUpdater<KafkaState> {

	private static final long serialVersionUID = -3657719900919830955L;
	private String messageFieldName;
	private String topicFieldName;
	private String keyFieldName;

	public KafkaStateUpdater(String messageFieldName, String topicFieldName, String keyFieldName) {
		this.topicFieldName = topicFieldName;
		this.keyFieldName = keyFieldName;
		this.messageFieldName = messageFieldName;		
	}

	@Override
	public void updateState(KafkaState state, List<TridentTuple> tuples,
			TridentCollector collector) {
		for (TridentTuple t : tuples) {
			try{
				if(t.size() > 0) {
					String topic = (String) t.getStringByField(topicFieldName);
					String key = (String) t.getStringByField(keyFieldName);
					String message = t.getStringByField(messageFieldName);
					
					state.enqueue(new KeyedMessage<String, String>(topic, key, message));
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}

	}

}
