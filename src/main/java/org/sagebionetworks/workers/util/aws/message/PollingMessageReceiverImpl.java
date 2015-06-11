package org.sagebionetworks.workers.util.aws.message;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.workers.util.progress.ProgressCallback;
import org.sagebionetworks.workers.util.progress.ThrottlingProgressCallback;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

/**
 * A MessageReceiver that uses long polling to fetch messages from AWS SQS.
 *
 */
public class PollingMessageReceiverImpl implements MessageReceiver {
	
	private static final Logger log = LogManager.getLogger(PollingMessageReceiverImpl.class);
	
	/*
	 * The maximum amount of time in seconds that this receiver will wait for a message to appear in the queue.
	 */
	public static int MAX_MESSAGE_POLL_TIME_SEC = 20;
	/*
	 * Since this receiver does long polling for messages we need to ensure semaphore lock timeouts are not less than poll time.
	 */
	public static int MIN_SEMAPHORE_LOCK_TIMEOUT_SEC = MAX_MESSAGE_POLL_TIME_SEC*2;
	
	AmazonSQSClient amazonSQSClient;
	MessageQueue messageQueue;
	Integer messageVisibilityTimeoutSec;
	Integer waitTimeSec;
	MessageDrivenRunner runner;
	long progressThrottleFrequencyMS;
	
	public PollingMessageReceiverImpl(AmazonSQSClient amazonSQSClient, PollingMessageReceiverConfiguration config) {
		super();
		this.amazonSQSClient = amazonSQSClient;
		if(config == null){
			throw new IllegalArgumentException("PollingMessageReceiverConfiguration cannot be null");
		}
		if(config.getMessageQueue() == null){
			throw new IllegalArgumentException("PollingMessageReceiverConfiguration.messageQueue cannot be null");
		}
		if(config.getMessageVisibilityTimeoutSec() == null){
			throw new IllegalArgumentException("PollingMessageReceiverConfiguration.messageVisibilityTimeoutSec cannot be null");
		}
		if(config.getSemaphoreLockTimeoutSec() == null){
			throw new IllegalArgumentException("PollingMessageReceiverConfiguration.semaphoreLockTimeoutSec cannot be null");
		}
		if(config.getSemaphoreLockTimeoutSec() < MIN_SEMAPHORE_LOCK_TIMEOUT_SEC){
			throw new IllegalArgumentException("PollingMessageReceiverConfiguration.semaphoreLockTimeoutSec must be at least "+MIN_SEMAPHORE_LOCK_TIMEOUT_SEC+" seconds.");
		}
		if(config.getRunner() == null){
			throw new IllegalArgumentException("PollingMessageReceiverConfiguration.runner cannot be null");
		}
		this.messageQueue = config.getMessageQueue();
		this.messageVisibilityTimeoutSec = config.getMessageVisibilityTimeoutSec();
		this.runner = config.getRunner();
		this.progressThrottleFrequencyMS = (config.getSemaphoreLockTimeoutSec()*1000)/3;
	}

	@Override
	public void run(final ProgressCallback<Message> containerProgressCallback) {
		ReceiveMessageRequest request = new ReceiveMessageRequest();
		request.setMaxNumberOfMessages(1);
		request.setQueueUrl(this.messageQueue.getQueueUrl());
		request.setVisibilityTimeout(this.messageVisibilityTimeoutSec);
		request.setWaitTimeSeconds(MAX_MESSAGE_POLL_TIME_SEC);
		ReceiveMessageResult results = this.amazonSQSClient.receiveMessage(request);
		if(results != null){
			List<Message> messages = results.getMessages();
			if(messages != null && !messages.isEmpty()){
				if(messages.size() != 1){
					throw new IllegalStateException("Expeced only one message but received: "+messages.size());
				}
				final Message message = messages.get(0);
				if(message == null){
					throw new IllegalStateException("Message list contains a null message");
				}
				// before we pass the message to the runner refresh the progress
				containerProgressCallback.progressMade(message);
				boolean deleteMessage = true;
				try{
					// Let the runner handle the message.
					runner.run(new ThrottlingProgressCallback<Message>(new ProgressCallback<Message>() {

						@Override
						public void progressMade(Message t) {
							// let the container know progress was made
							containerProgressCallback.progressMade(message);
							resetMessageVisibilityTimeout(message);
						}
					}, progressThrottleFrequencyMS), message);
					
				}catch(RecoverableMessageException e){
					// this is the only case where we do not delete the message.
					deleteMessage = false;
					if(log.isDebugEnabled()){
						log.debug("Message will be returned to the queue", e);
					}
				}finally{
					if(deleteMessage){
						deleteMessage(message);
					}
				}
			}
		}
	}

	/**
	 * Delete the given message from the queue.
	 * @param message
	 */
	protected void deleteMessage(Message message){
		DeleteMessageRequest deleteRequest = new DeleteMessageRequest();
		deleteRequest.setQueueUrl(this.messageQueue.getQueueUrl());
		deleteRequest.setReceiptHandle(message.getReceiptHandle());
		if(log.isTraceEnabled()){
			log.trace("Deleting message: "+deleteRequest.toString());
		}
		this.amazonSQSClient.deleteMessage(deleteRequest);
	}
	
	/**
	 * Reset the visibility timeout of the given message. Called when progress is made for a given message.
	 * @param message
	 */
	protected void resetMessageVisibilityTimeout(Message message){
		ChangeMessageVisibilityRequest changeRequest = new ChangeMessageVisibilityRequest();
		changeRequest.setQueueUrl(this.messageQueue.getQueueUrl());
		changeRequest.setReceiptHandle(message.getReceiptHandle());
		changeRequest.setVisibilityTimeout(this.messageVisibilityTimeoutSec);
		this.amazonSQSClient.changeMessageVisibility(changeRequest);
	}
	
	@Override
	public void attemptToEmptyQueue() {
		this.amazonSQSClient.purgeQueue(new PurgeQueueRequest(this.messageQueue.getQueueUrl()));
	}

}
