package org.sagebionetworks.workers.util.aws.message;

import java.util.Arrays;
import java.util.LinkedList;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.sagebionetworks.workers.util.progress.ProgressCallback;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

public class PollingMessageReceiverImplTest {

	AmazonSQSClient mockAmazonSQSClient;
	MessageDrivenRunner mockRunner;
	ProgressCallback<Message> mockProgressCallback;

	HasQueueUrl mockHasQueueUrl;
	PollingMessageReceiverConfiguration config;
	String queueUrl;
	int messageVisibilityTimeoutSec;
	int semaphoreLockTimeoutSec;

	Message message;

	@Before
	public void before() {
		queueUrl = "aQueueUrl";
		messageVisibilityTimeoutSec = 60;
		semaphoreLockTimeoutSec = 60;

		mockAmazonSQSClient = Mockito.mock(AmazonSQSClient.class);
		mockHasQueueUrl = Mockito.mock(HasQueueUrl.class);
		mockRunner = Mockito.mock(MessageDrivenRunner.class);
		mockProgressCallback = Mockito.mock(ProgressCallback.class);

		when(mockHasQueueUrl.getQueueUrl()).thenReturn(queueUrl);

		// setup for a single message.
		ReceiveMessageResult results = new ReceiveMessageResult();
		message = new Message();
		message.setReceiptHandle("handle");
		results.setMessages(Arrays.asList(message));
		when(
				mockAmazonSQSClient
						.receiveMessage(any(ReceiveMessageRequest.class)))
				.thenReturn(results);

		config = new PollingMessageReceiverConfiguration();
		config.setHasQueueUrl(mockHasQueueUrl);
		config.setRunner(mockRunner);
		config.setMessageVisibilityTimeoutSec(messageVisibilityTimeoutSec);
		config.setSemaphoreLockTimeoutSec(semaphoreLockTimeoutSec);
	}

	@Test
	public void testHappyConstructor() {
		new PollingMessageReceiverImpl(mockAmazonSQSClient, config);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNullClient() {
		new PollingMessageReceiverImpl(null, config);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSemaphoreLockTooSmall() {
		config.setSemaphoreLockTimeoutSec(39);
		new PollingMessageReceiverImpl(mockAmazonSQSClient, config);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSemaphoreLockLessThanVisisibleTimeout() {
		config.setSemaphoreLockTimeoutSec(config
				.getMessageVisibilityTimeoutSec() - 1);
		new PollingMessageReceiverImpl(mockAmazonSQSClient, config);
	}

	@Test
	public void testRunNullMessages() throws Throwable {
		ReceiveMessageResult results = new ReceiveMessageResult();
		when(
				mockAmazonSQSClient
						.receiveMessage(any(ReceiveMessageRequest.class)))
				.thenReturn(results);

		PollingMessageReceiverImpl receiver = new PollingMessageReceiverImpl(
				mockAmazonSQSClient, config);

		// call under test
		receiver.run(mockProgressCallback);
		verify(mockProgressCallback, never()).progressMade(any(Message.class));
		verify(mockRunner, never()).run(any(ProgressCallback.class),
				any(Message.class));
	}

	@Test
	public void testRunEmptyMessages() throws Throwable {
		ReceiveMessageResult results = new ReceiveMessageResult();
		results.setMessages(new LinkedList<Message>());
		when(
				mockAmazonSQSClient
						.receiveMessage(any(ReceiveMessageRequest.class)))
				.thenReturn(results);

		PollingMessageReceiverImpl receiver = new PollingMessageReceiverImpl(
				mockAmazonSQSClient, config);

		// call under test
		receiver.run(mockProgressCallback);
		verify(mockProgressCallback, never()).progressMade(any(Message.class));
		verify(mockRunner, never()).run(any(ProgressCallback.class),
				any(Message.class));
	}

	@Test(expected = IllegalStateException.class)
	public void testRunTooMessages() throws Exception {
		ReceiveMessageResult results = new ReceiveMessageResult();
		Message message = new Message();
		results.setMessages(Arrays.asList(message, message));
		when(
				mockAmazonSQSClient
						.receiveMessage(any(ReceiveMessageRequest.class)))
				.thenReturn(results);

		PollingMessageReceiverImpl receiver = new PollingMessageReceiverImpl(
				mockAmazonSQSClient, config);

		// call under test
		receiver.run(mockProgressCallback);
	}

	@Test
	public void testOneMessage() throws Throwable {
		PollingMessageReceiverImpl receiver = new PollingMessageReceiverImpl(
				mockAmazonSQSClient, config);

		// call under test
		receiver.run(mockProgressCallback);
		verify(mockProgressCallback, times(1)).progressMade(any(Message.class));
		verify(mockRunner, times(1)).run(any(ProgressCallback.class),
				any(Message.class));
		// The message should be deleted
		DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
		deleteMessageRequest.setQueueUrl(queueUrl);
		deleteMessageRequest.setReceiptHandle(message.getReceiptHandle());
		verify(mockAmazonSQSClient, times(1)).deleteMessage(
				deleteMessageRequest);
	}

	@Test
	public void testMessageDeleteOnException()
			throws Throwable {
		// setup the runner to throw an unknown exception
		doThrow(new IllegalArgumentException("Something was null")).when(
				mockRunner)
				.run(any(ProgressCallback.class), any(Message.class));

		PollingMessageReceiverImpl receiver = new PollingMessageReceiverImpl(
				mockAmazonSQSClient, config);

		// call under test
		try {
			receiver.run(mockProgressCallback);
			fail("Should have thrown an exception");
		} catch (IllegalArgumentException e) {
			// expected
		}
		// The message should be deleted for any non-RecoverableMessageException
		DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
		deleteMessageRequest.setQueueUrl(queueUrl);
		deleteMessageRequest.setReceiptHandle(message.getReceiptHandle());
		verify(mockAmazonSQSClient, times(1)).deleteMessage(
				deleteMessageRequest);
	}

	@Test
	public void testMessageNoDeleteRecoverableMessageException()
			throws Throwable {
		// setup the runner to throw a RecoverableMessageException
		doThrow(new RecoverableMessageException("Try again later.")).when(
				mockRunner)
				.run(any(ProgressCallback.class), any(Message.class));

		PollingMessageReceiverImpl receiver = new PollingMessageReceiverImpl(
				mockAmazonSQSClient, config);

		// call under test
		receiver.run(mockProgressCallback);
		// The message should not be deleted for RecoverableMessageException
		verify(mockAmazonSQSClient, never()).deleteMessage(
				any(DeleteMessageRequest.class));
		// The visibility of the message should be reset
		ChangeMessageVisibilityRequest expectedRequest = new ChangeMessageVisibilityRequest(
				this.queueUrl, this.message.getReceiptHandle(),
				PollingMessageReceiverImpl.RETRY_MESSAGE_VISIBILITY_TIMEOUT_SEC);
		verify(mockAmazonSQSClient, times(1)).changeMessageVisibility(
				expectedRequest);
	}

	@Test
	public void testProgressCallbackThrottle()
			throws Throwable {
		int timeout = 40;
		config.setMessageVisibilityTimeoutSec(timeout);
		config.setSemaphoreLockTimeoutSec(timeout);
		// mock the runner to call progressMade.
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				ProgressCallback<Message> callback = (ProgressCallback<Message>) invocation
						.getArguments()[0];
				Message message = (Message) invocation.getArguments()[1];

				// call back
				callback.progressMade(message);
				// should be throttled
				callback.progressMade(message);
				// should be throttled
				callback.progressMade(message);
				// Now sleep and callback again
				Thread.sleep(14000);
				// should not be throttled.
				callback.progressMade(message);
				return null;
			}
		}).when(mockRunner)
				.run(any(ProgressCallback.class), any(Message.class));

		PollingMessageReceiverImpl receiver = new PollingMessageReceiverImpl(
				mockAmazonSQSClient, config);

		// call under test
		receiver.run(mockProgressCallback);
		/*
		 * Progress should be made for the following: One time after the message
		 * received. The first time the runner calls the callback. The last time
		 * the runner calls the callback after waiting for 15 seconds. The rest
		 * of the calls should be throttled.
		 */
		verify(mockProgressCallback, times(3)).progressMade(any(Message.class));
		ChangeMessageVisibilityRequest changeRequset = new ChangeMessageVisibilityRequest();
		changeRequset.setQueueUrl(queueUrl);
		changeRequset.setReceiptHandle(message.getReceiptHandle());
		changeRequset.setVisibilityTimeout(timeout);
		/*
		 * changeMessageVisibility() should be made for the following: The first
		 * time the runner calls the callback. The last time the runner calls
		 * the callback after waiting for 15 seconds. The rest of the calls
		 * should be throttled.
		 */
		verify(mockAmazonSQSClient, times(2)).changeMessageVisibility(
				changeRequset);
	}

	@Test
	public void testAttemptToEmptyQueue() {
		// Simulate two batches to delete
		ReceiveMessageResult pageOne = new ReceiveMessageResult();
		pageOne.setMessages(Arrays.asList(new Message().withMessageId("id1")
				.withReceiptHandle("h1"), new Message().withMessageId("id2")
				.withReceiptHandle("h2")));
		ReceiveMessageResult pageTwo = new ReceiveMessageResult();
		pageTwo.setMessages(Arrays.asList(new Message().withMessageId("id3")
				.withReceiptHandle("h3")));
		// page three is empty.
		ReceiveMessageResult pageThree = new ReceiveMessageResult();
		pageThree.setMessages(new LinkedList<Message>());
		when(
				mockAmazonSQSClient
						.receiveMessage(any(ReceiveMessageRequest.class)))
				.thenReturn(pageOne, pageTwo, pageThree);

		PollingMessageReceiverImpl receiver = new PollingMessageReceiverImpl(
				mockAmazonSQSClient, config);

		// call under test
		receiver.attemptToEmptyQueue();
		// each page should be deleted as a batch.
		verify(mockAmazonSQSClient, times(2)).deleteMessageBatch(
				any(DeleteMessageBatchRequest.class));
	}
}
