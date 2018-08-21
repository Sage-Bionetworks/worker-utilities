package org.sagebionetworks.workers.util.aws.message;

import java.util.List;

import org.sagebionetworks.workers.util.Gate;
import org.sagebionetworks.workers.util.semaphore.SemaphoreGatedRunnerConfiguration;

/**
 * Wrapper for all of the Configuration needed to create a MessageDrivenWorkerStack.
 *
 */
public class MessageDrivenWorkerStackConfiguration {
	
	MessageQueueConfiguration messageQueueConfiguration;
	PollingMessageReceiverConfiguration pollingMessageReceiverConfiguration;
	SemaphoreGatedRunnerConfiguration semaphoreGatedRunnerConfiguration;
	Gate gate;

	public MessageDrivenWorkerStackConfiguration() {
		messageQueueConfiguration = new MessageQueueConfiguration();
		pollingMessageReceiverConfiguration = new PollingMessageReceiverConfiguration();
		semaphoreGatedRunnerConfiguration = new SemaphoreGatedRunnerConfiguration();
	}

	public MessageQueueConfiguration getMessageQueueConfiguration() {
		return messageQueueConfiguration;
	}

	public PollingMessageReceiverConfiguration getPollingMessageReceiverConfiguration() {
		return pollingMessageReceiverConfiguration;
	}

	public SemaphoreGatedRunnerConfiguration getSemaphoreGatedRunnerConfiguration() {
		return semaphoreGatedRunnerConfiguration;
	}
	
	/**
	 * The name of queue.
	 * 
	 * @param queueName
	 */
	public void setQueueName(String queueName) {
		messageQueueConfiguration.setQueueName(queueName);
	}

	/**
	 * An optional parameter that will cause an CloudWatch Alarm to be raised if the oldest message in the queue exceeds this value.
	 * @param oldestMessageInQueueAlarmThresholdSec
	 */
	public void setOldestMessageInQueueAlarmThresholdSec(Integer oldestMessageInQueueAlarmThresholdSec){
		messageQueueConfiguration.setOldestMessageInQueueAlarmThresholdSec(oldestMessageInQueueAlarmThresholdSec);
	}

	/**
	 * ARN of the AWS resource to be notified when the alarm is triggered
	 * @param alarmNotificationARN
	 */
	public void setOldestMessageInQueueAlarmNotificationARN(String alarmNotificationARN){
		messageQueueConfiguration.setOldestMessageInQueueAlarmNotificationARN(alarmNotificationARN);
	}
	
	/**
	 * The runner that handles a message pulled from the queue.
	 * @param runner
	 */
	public void setRunner(MessageDrivenRunner runner) {
		pollingMessageReceiverConfiguration.setRunner(runner);
	}
	
	/**
	 * The semaphore lock key that must be held in order to run the runner.
	 * @param lockKey
	 */
	public void setSemaphoreLockKey(String lockKey){
		semaphoreGatedRunnerConfiguration.setLockKey(lockKey);
	}
	
	/**
	 * The maximum number of concurrent locks that can be issued for the given
	 * semaphore key. If the runner is expected to be a singleton, then set this
	 * value to one.
	 * 
	 * @param maxLockCount
	 */
	public void setSemaphoreMaxLockCount(int maxLockCount) {
		semaphoreGatedRunnerConfiguration.setMaxLockCount(maxLockCount);
	}
	
	/**
	 * The lock timeout in seconds for both the MessageVisibilityTimeoutSec and SemaphoreLockTimeoutSec.
	 * @param timeoutSec
	 */
	public void setSemaphoreLockAndMessageVisibilityTimeoutSec(Integer timeoutSec){
		semaphoreGatedRunnerConfiguration.setLockTimeoutSec(timeoutSec);
		messageQueueConfiguration.setDefaultMessageVisibilityTimeoutSec(timeoutSec);
		pollingMessageReceiverConfiguration.setMessageVisibilityTimeoutSec(timeoutSec);
		pollingMessageReceiverConfiguration.setSemaphoreLockTimeoutSec(timeoutSec);
	}
	
	/**
	 * An optional parameter. When set, each run will only occur if the provided {@link Gate#canRun()} returns true.
	 * @return
	 */
	public Gate getGate() {
		return gate;
	}

	/**
	 * An optional parameter. When set, each run will only occur if the provided {@link Gate#canRun()} returns true.
	 * @param gate
	 */
	public void setGate(Gate gate) {
		this.gate = gate;
		pollingMessageReceiverConfiguration.setGate(gate);
	}
	
	/**
	 * An optional parameter used to subscribe the queue to receive messages
	 * from each topic named in the list.
	 * 
	 * @param topicNamesToSubscribe
	 */
	public void setTopicNamesToSubscribe(List<String> topicNamesToSubscribe){
		messageQueueConfiguration.setTopicNamesToSubscribe(topicNamesToSubscribe);
	}
	
	/**
	 * Optional parameter used to configure this queue to use setup a dead
	 * letter queue for failed messages.
	 * 
	 * If this is set, then {@link #setDeadLetterMaxFailureCount(Integer)} must also be
	 * set.
	 * 
	 * @param deadLetterQueueName
	 *            The name of the dead letter queue where failed messages should
	 *            be pushed when the max failure count is exceeded.
	 */
	public void setDeadLetterQueueName(String deadLetterQueueName) {
		messageQueueConfiguration.setDeadLetterQueueName(deadLetterQueueName);
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
	public void setDeadLetterMaxFailureCount(Integer maxFailureCount) {
		messageQueueConfiguration.setMaxFailureCount(maxFailureCount);
	}
	
	/**
	 * When set to true a heartbeat progress event will automatically be generated
	 * as long as  the runner is running.
	 * Defaults to false (no heartbeat).
	 * @param useProgressHeartbeat
	 */
	public void setUseProgressHeartbeat(boolean useProgressHeartbeat) {
		this.semaphoreGatedRunnerConfiguration.setUseProgressHeartbeat(useProgressHeartbeat);
	}

}
