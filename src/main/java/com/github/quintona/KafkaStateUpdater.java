package com.github.quintona;

import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

public class KafkaStateUpdater extends BaseStateUpdater<KafkaState> {

	private static final long serialVersionUID = -3657719900919830955L;
	private String messageFieldName;

	public KafkaStateUpdater(String messageFieldName) {
		this.messageFieldName = messageFieldName;
	}

	@Override
	public void updateState(KafkaState state, List<TridentTuple> tuples,
			TridentCollector collector) {
		for (TridentTuple t : tuples) {
			try{
				if(t.size() > 0)
					state.enqueue(t.getStringByField(messageFieldName));
			}catch(Exception e){
				e.printStackTrace();
			}
		}

	}

}
