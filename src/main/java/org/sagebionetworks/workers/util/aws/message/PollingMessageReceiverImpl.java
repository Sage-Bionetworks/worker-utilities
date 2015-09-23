package org.sagebionetworks.workers.util.aws.message;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sagebionetworks.workers.util.Gate;
import org.sagebionetworks.workers.util.progress.ProgressCallback;
import org.sagebionetworks.workers.util.progress.ProgressingRunner;
import org.sagebionetworks.workers.util.progress.ThrottlingProgressCallback;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

/**
 * A MessageReceiver that uses long polling to fetch messages from AWS SQS.
 * 
 */
public class PollingMessageReceiverImpl implements ProgressingRunner<Message> {

	private static final Logger log = LogManager
			.getLogger(PollingMessageReceiverImpl.class);

	/*
	 * The maximum amount of time in seconds that this receiver will wait for a
	 * message to appear in the queue.
	 */
	public static int MAX_MESSAGE_POLL_TIME_SEC = 2;
	/*
	 * Used for message that failed but should be returned to the queue.  For this case
	 * we want to be able to retry the message quickly, so it is set to 5 seconds. 
	 */
	public static int RETRY_MESSAGE_VISIBILITY_TIMEOUT_SEC = 5;
	/*
	 * Since this receiver does long polling for messages we need to ensure
	 * semaphore lock timeouts are not less than poll time.
	 */
	public static int MIN_SEMAPHORE_LOCK_TIMEOUT_SEC = MAX_MESSAGE_POLL_TIME_SEC * 2;

	AmazonSQSClient amazonSQSClient;
	String messageQueueUrl;
	Integer messageVisibilityTimeoutSec;
	Integer waitTimeSec;
	MessageDrivenRunner runner;
	long progressThrottleFrequencyMS;
	Gate gate;

	/**
	 * 
	 * @param amazonSQSClient
	 *            An AmazonSQSClient configured with credentials.
	 * @param config
	 *            Configuration information for this message receiver.
	 */
	public PollingMessageReceiverImpl(AmazonSQSClient amazonSQSClient,
			PollingMessageReceiverConfiguration config) {
		super();
		if (amazonSQSClient == null) {
			throw new IllegalArgumentException("AmazonSQSClient cannot be null");
		}
		this.amazonSQSClient = amazonSQSClient;
		if (config == null) {
			throw new IllegalArgumentException(
					"PollingMessageReceiverConfiguration cannot be null");
		}
		if (config.getHasQueueUrl() == null) {
			throw new IllegalArgumentException(
					"PollingMessageReceiverConfiguration.hasQueueUrl cannot be null");
		}
		if (config.getHasQueueUrl().getQueueUrl() == null) {
			throw new IllegalArgumentException(
					"PollingMessageReceiverConfiguration.hasQueueUrl.queueUrl cannot be null");
		}
		if (config.getMessageVisibilityTimeoutSec() == null) {
			throw new IllegalArgumentException(
					"PollingMessageReceiverConfiguration.messageVisibilityTimeoutSec cannot be null");
		}
		if (config.getSemaphoreLockTimeoutSec() == null) {
			throw new IllegalArgumentException(
					"PollingMessageReceiverConfiguration.semaphoreLockTimeoutSec cannot be null");
		}
		if (config.getSemaphoreLockTimeoutSec() < MIN_SEMAPHORE_LOCK_TIMEOUT_SEC) {
			throw new IllegalArgumentException(
					"PollingMessageReceiverConfiguration.semaphoreLockTimeoutSec must be at least "
							+ MIN_SEMAPHORE_LOCK_TIMEOUT_SEC + " seconds.");
		}
		if (config.getSemaphoreLockTimeoutSec() < config
				.getMessageVisibilityTimeoutSec()) {
			throw new IllegalArgumentException(
					"PollingMessageReceiverConfiguration.semaphoreLockTimeoutSec cannot be less than pollingMessageReceiverConfiguration.messageVisibilityTimeoutSec ");
		}
		if (config.getRunner() == null) {
			throw new IllegalArgumentException(
					"PollingMessageReceiverConfiguration.runner cannot be null");
		}
		this.messageQueueUrl = config.getHasQueueUrl().getQueueUrl();
		this.messageVisibilityTimeoutSec = config
				.getMessageVisibilityTimeoutSec();
		this.runner = config.getRunner();
		this.progressThrottleFrequencyMS = (config.getSemaphoreLockTimeoutSec() * 1000) / 3;
		this.gate = config.getGate();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.sagebionetworks.workers.util.progress.ProgressingRunner#run(org.
	 * sagebionetworks.workers.util.progress.ProgressCallback)
	 */
	@Override
	public void run(final ProgressCallback<Message> containerProgressCallback) throws Exception {
		Message message = null;
		do {
			if (gate != null && !gate.canRun()) {
				log.info(gate.getClass().getSimpleName() +" is closed for " + runner.getClass().getSimpleName());
				break;
			}
			message = pollForMessage();
			if(message != null){
				processMessage(containerProgressCallback, message);
			}
		} while (message != null);
		log.info("There is no more messages for "+runner.getClass().getSimpleName());
	}
	
	/**
	 * Poll for a single message.
	 * @return
	 */
	private Message pollForMessage(){
		log.info("Getting message for " + runner.getClass().getSimpleName());
		ReceiveMessageRequest request = new ReceiveMessageRequest();
		request.setMaxNumberOfMessages(1);
		request.setQueueUrl(this.messageQueueUrl);
		request.setVisibilityTimeout(this.messageVisibilityTimeoutSec);
		request.setWaitTimeSeconds(0);
		// Poll for one message.
		ReceiveMessageResult results = this.amazonSQSClient
				.receiveMessage(request);
		if (results != null) {
			List<Message> messages = results.getMessages();
			if (messages != null && !messages.isEmpty()) {
				if (messages.size() != 1) {
					throw new IllegalStateException(
							"Expected only one message but received: "
									+ messages.size());
				}
				final Message message = messages.get(0);
				if (message == null) {
					throw new IllegalStateException(
							"Message list contains a null message");
				}
				return message;
			}
		}
		// no message for you
		return null;
	}

	/**
	 * Process a single message.
	 * @param containerProgressCallback
	 * @param message
	 * @throws Exception
	 */
	private void processMessage(
			final ProgressCallback<Message> containerProgressCallback,
			final Message message) throws Exception {
		log.info("Processing message for "+runner.getClass().getSimpleName());
		// before we pass the message to the runner refresh the progress
		containerProgressCallback.progressMade(message);
		boolean deleteMessage = true;
		try {
			// Let the runner handle the message.
			runner.run(new ThrottlingProgressCallback<Message>(
					new ProgressCallback<Message>() {

						@Override
						public void progressMade(Message t) {
							// let the container know progress was made
							containerProgressCallback
									.progressMade(message);
							resetMessageVisibilityTimeout(message);
						}
					}, progressThrottleFrequencyMS), message);

		} catch (RecoverableMessageException e) {
			// this is the only case where we do not delete the message.
			deleteMessage = false;
			if (log.isDebugEnabled()) {
				log.debug("Message will be returned to the queue", e);
			}
			// Ensure this message is visible again in 5 seconds
			resetMessageVisibilityTimeout(message, RETRY_MESSAGE_VISIBILITY_TIMEOUT_SEC);
		} finally {
			if (deleteMessage) {
				deleteMessage(message);
			}
		}
	}

	/**
	 * Delete the given message from the queue.
	 * 
	 * @param message
	 */
	protected void deleteMessage(Message message) {
		this.amazonSQSClient.deleteMessage(new DeleteMessageRequest(this.messageQueueUrl, message.getReceiptHandle()));
	}

	/**
	 * Reset the visibility timeout of the given message using the configured messageVisibilityTimeoutSec. Called when progress
	 * is made for a given message.
	 * 
	 * @param message
	 */
	protected void resetMessageVisibilityTimeout(Message message) {
		resetMessageVisibilityTimeout(message, this.messageVisibilityTimeoutSec);
	}
	
	/**
	 * Reset the visibility timeout of the given message to the provided using the provided visibilityTimeoutSec.
	 * @param message
	 * @param visibilityTimeoutSec
	 */
	protected void resetMessageVisibilityTimeout(Message message, int visibilityTimeoutSec) {
		ChangeMessageVisibilityRequest changeRequest = new ChangeMessageVisibilityRequest();
		changeRequest.setQueueUrl(this.messageQueueUrl);
		changeRequest.setReceiptHandle(message.getReceiptHandle());
		changeRequest.setVisibilityTimeout(visibilityTimeoutSec);
		this.amazonSQSClient.changeMessageVisibility(changeRequest);
	}
}
