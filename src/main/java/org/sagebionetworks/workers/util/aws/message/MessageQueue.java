package org.sagebionetworks.workers.util.aws.message;


/**
 * Abstraction for a message queue.  Each queue should setup the AWS SQS used by this queue.
 *
 */
public interface MessageQueue extends HasQueueUrl {

	/**
	 * The name of the SQS queue.
	 * @return
	 */
	public String getQueueName();
	
	/**
	 * Is this queue enabled?
	 * @return
	 */
	public boolean isEnabled();
	
	/**
	 * The name of the dead letter queue
	 * @return
	 */
	public String getDeadLetterQueueName();
	
	/**
	 * The URL of the dead letter queue associated with this queue
	 * @return
	 */
	public String getDeadLetterQueueUrl();
	
	/**
	 * The number of times a msg is received before moving to the dead letter queue
	 * @return
	 */
	public Integer getMaxReceiveCount();

}
