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
	boolean isEnabled = true;
	String deadLetterQueueName;
	Integer maxFailureCount;
	Integer defaultMessageVisibilityTimeoutSec;
	Integer oldestMessageInQueueAlarmThresholdSec;

	String alarmNotificationARN;

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

	/**
	 * @see {@link MessageQueueConfiguration#setQueueName(String)}
	 * @return
	 */
	public String getQueueName() {
		return queueName;
	}

	/**
	 * @see {@link MessageQueueConfiguration#getTopicNamesToSubscribe()}
	 * @return
	 */
	public List<String> getTopicNamesToSubscribe() {
		return topicNamesToSubscribe;
	}

	/**
	 * @see {@link MessageQueueConfiguration#isEnabled()}
	 * @return
	 */
	public boolean isEnabled() {
		return isEnabled;
	}

	/**
	 * @see {@link MessageQueueConfiguration#getDeadLetterQueueName()}
	 * @return
	 */
	public String getDeadLetterQueueName() {
		return deadLetterQueueName;
	}

	/**
	 * @see {@link MessageQueueConfiguration#getMaxFailureCount()}
	 * @return
	 */
	public Integer getMaxFailureCount() {
		return maxFailureCount;
	}

	/**
	 * The default messages visibility timeout in seconds for the queue.
	 * 
	 * @param timeoutSec
	 */
	public void setDefaultMessageVisibilityTimeoutSec(Integer timeoutSec) {
		this.defaultMessageVisibilityTimeoutSec = timeoutSec;
	}

	/**
	 * The default messages visibility timeout in seconds for the queue.
	 * @return
	 */
	public Integer getDefaultMessageVisibilityTimeoutSec() {
		return defaultMessageVisibilityTimeoutSec;
	}

	/**
	 * An optional parameter that will cause an CloudWatch Alarm to be raised if the oldest message in the queue exceeds this value.
	 * @return
	 */
	public Integer getOldestMessageInQueueAlarmThresholdSec() {
		return oldestMessageInQueueAlarmThresholdSec;
	}

	/**
	 * An optional parameter that will cause an CloudWatch Alarm to be raised if the oldest message in the queue exceeds this value.
	 * @param oldestMessageInQueueAlarmThresholdSec
	 */
	public void setOldestMessageInQueueAlarmThresholdSec(Integer oldestMessageInQueueAlarmThresholdSec) {
		this.oldestMessageInQueueAlarmThresholdSec = oldestMessageInQueueAlarmThresholdSec;
	}

	/**
	 * ARN of the AWS resource to be notified when the alarm is triggered
	 * @return
	 */
	public String getOldestMessageInQueueAlarmNotificationARN() {
		return alarmNotificationARN;
	}

	/**
	 * ARN of the AWS resource to be notified when the alarm is triggered
	 * @param alarmNotificationARN
	 */
	public void setOldestMessageInQueueAlarmNotificationTopicARN(String alarmNotificationARN) {
		this.alarmNotificationARN = alarmNotificationARN;
	}
}
