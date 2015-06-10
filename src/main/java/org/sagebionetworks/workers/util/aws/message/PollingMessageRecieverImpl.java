package org.sagebionetworks.workers.util.aws.message;

import org.sagebionetworks.workers.util.progress.ProgressCallback;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;

public class PollingMessageRecieverImpl implements MessageReciever {
	
	AmazonSQSClient amazonSQSClient;
	
	
	public PollingMessageRecieverImpl(AmazonSQSClient amazonSQSClient) {
		super();
		this.amazonSQSClient = amazonSQSClient;
	}

	@Override
	public void run(ProgressCallback<Message> progressCallback) {

		
	}

	@Override
	public void attemptToEmptyQueue() {
		// TODO Auto-generated method stub
		
	}

}
