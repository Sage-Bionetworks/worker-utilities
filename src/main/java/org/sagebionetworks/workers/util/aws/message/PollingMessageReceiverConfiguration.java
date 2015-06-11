package org.sagebionetworks.workers.util.aws.message;

/**
 * Configuration information for the PollingMessageReceiver
 * 
 */
public class PollingMessageReceiverConfiguration {

	HasQueueUrl hasQueueUrl;
	Integer messageVisibilityTimeoutSec;
	Integer semaphoreLockTimeoutSec;
	MessageDrivenRunner runner;

	/**
	 * The amount of time in seconds that a fetched message will be in flight
	 * before returning to the queue.
	 * 
	 * @return
	 */
	public Integer getMessageVisibilityTimeoutSec() {
		return messageVisibilityTimeoutSec;
	}

	/**
	 * The amount of time in seconds that a fetched message will be in flight
	 * before returning to the queue.
	 * 
	 * @param messageVisibilityTimeoutSec
	 */
	public void setMessageVisibilityTimeoutSec(
			Integer messageVisibilityTimeoutSec) {
		this.messageVisibilityTimeoutSec = messageVisibilityTimeoutSec;
	}

	/**
	 * The amount of time in seconds that the semaphore lock being held for this
	 * message has before timing out. This message receiver will do long polling
	 * and can be idle for 20 seconds while waiting for a message to appear on
	 * the queue. Therefore, the held semaphore lock timeout must be at least
	 * double the maximum poll time of 20 seconds ( > 40 seconds).
	 */
	public Integer getSemaphoreLockTimeoutSec() {
		return semaphoreLockTimeoutSec;
	}

	/**
	 * The amount of time in seconds that the semaphore lock being held for this
	 * message has before timing out. This message receiver will do long polling
	 * and can be idle for 20 seconds while waiting for a message to appear on
	 * the queue. Therefore, the held semaphore lock timeout must be at least
	 * double the maximum poll time of 20 seconds ( > 40 seconds).
	 * 
	 * @param semaphoreLockTimeoutSec
	 */
	public void setSemaphoreLockTimeoutSec(Integer semaphoreLockTimeoutSec) {
		this.semaphoreLockTimeoutSec = semaphoreLockTimeoutSec;
	}

	/**
	 * The runner that handles a message pull from the queue.
	 * 
	 * @return
	 */
	public MessageDrivenRunner getRunner() {
		return runner;
	}

	/**
	 * The runner that handles a message pull from the queue.
	 * @param runner
	 */
	public void setRunner(MessageDrivenRunner runner) {
		this.runner = runner;
	}

	/**
	 * Provides the URL for the queue to long poll.
	 * @return
	 */
	public HasQueueUrl getHasQueueUrl() {
		return hasQueueUrl;
	}

	/**
	 * Provides the URL for the queue to long poll.
	 * @param hasQueueUrl
	 */
	public void setHasQueueUrl(HasQueueUrl hasQueueUrl) {
		this.hasQueueUrl = hasQueueUrl;
	}

}
