package org.sagebionetworks.workers.util.aws.message;


/**
 * Configuration information for the PollingMessageReceiver
 *
 */
public class PollingMessageReceiverConfiguration {
	
	MessageQueue messageQueue;
	Integer messageVisibilityTimeoutSec;
	Integer semaphoreLockTimeoutSec;
	MessageDrivenRunner runner;

	public MessageQueue getMessageQueue() {
		return messageQueue;
	}

	public void setMessageQueue(MessageQueue messageQueue) {
		this.messageQueue = messageQueue;
	}

	public Integer getMessageVisibilityTimeoutSec() {
		return messageVisibilityTimeoutSec;
	}

	public void setMessageVisibilityTimeoutSec(Integer messageVisibilityTimeoutSec) {
		this.messageVisibilityTimeoutSec = messageVisibilityTimeoutSec;
	}

	public Integer getSemaphoreLockTimeoutSec() {
		return semaphoreLockTimeoutSec;
	}

	public void setSemaphoreLockTimeoutSec(Integer semaphoreLockTimeoutSec) {
		this.semaphoreLockTimeoutSec = semaphoreLockTimeoutSec;
	}

	public MessageDrivenRunner getRunner() {
		return runner;
	}

	public void setRunner(MessageDrivenRunner runner) {
		this.runner = runner;
	}

}
