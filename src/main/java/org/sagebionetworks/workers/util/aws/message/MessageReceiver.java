package org.sagebionetworks.workers.util.aws.message;

import org.sagebionetworks.workers.util.progress.ProgressingRunner;

import com.amazonaws.services.sqs.model.Message;

/**
 * Abstraction for a MessageReciever that pull messages from an AWS SQS queue to
 * be processed by a {@link MessageDrivenRunner}. The MessageReciever is
 * expected to manage messages including pull, deleting, and refreshing message
 * visibility.
 * 
 */
public interface MessageReceiver extends ProgressingRunner<Message> {

	/**
	 * It is not possible to guarantee all message are removed from a queue.
	 * Instead, an attempt should be made to remove as many messages as possible
	 * from the queue.
	 */
	public void attemptToEmptyQueue();
}
