package org.sagebionetworks.workers.util.aws.message;

import java.util.List;

/**
 * Configuration information used to configure an AWS SQS queue with additional
 * parameters for registering the queue to receive message from an AWS SNS topic
 * 
 */
public class MessageQueueConfiguration {

	String queueName;
	List<String> topicNamesToSubscribe;
	boolean isEnabled;
	String deadLetterQueueName;
	Integer maxFailureCount;

	public MessageQueueConfiguration() {
	}

	/**
	 * The name of queue.
	 * 
	 * @param queueName
	 */
	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}

	/**
	 * An optional parameter used to subscribe this queue to receive messages
	 * from topics.
	 * 
	 * @param topicNamesToSubscribe
	 *            The name of each topic that this queue should subscribe to
	 *            receive messages from.
	 */
	public void setTopicNamesToSubscribe(List<String> topicNamesToSubscribe) {
		this.topicNamesToSubscribe = topicNamesToSubscribe;
	}

	/**
	 * Is this queue enabled?
	 * 
	 * @param isEnabled
	 */
	public void setEnabled(boolean isEnabled) {
		this.isEnabled = isEnabled;
	}

	/**
	 * Optional parameter used to configure this queue to use setup a dead
	 * letter queue for failed messages.
	 * 
	 * If this is set, then {@link #setMaxFailureCount(Integer)} must also be
	 * set.
	 * 
	 * @param deadLetterQueueName
	 *            The name of the dead letter queue where failed messages should
	 *            be pushed when the max failure count is exceeded.
	 */
	public void setDeadLetterQueueName(String deadLetterQueueName) {
		this.deadLetterQueueName = deadLetterQueueName;
	}

	/**
	 * An optional parameter used to configure this queue to forward failed
	 * messages to a dead letter queue.
	 * 
	 * If this is set then the {@link #setDeadLetterQueueName(Integer)} must
	 * also be set.
	 * 
	 * @param maxFailureCount
	 *            The maximum number of times a message should be retried before
	 *            before being pushed to the dead letter queue.
	 */
	public void setMaxFailureCount(Integer maxFailureCount) {
		this.maxFailureCount = maxFailureCount;
	}

}
