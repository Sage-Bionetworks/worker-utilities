package org.sagebionetworks.workers.util.aws.message;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.sagebionetworks.common.util.progress.ProgressCallback;
import org.sagebionetworks.common.util.progress.ProgressListener;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

public class PollingMessageReceiverImplTest {

	@Mock
	AmazonSQSClient mockAmazonSQSClient;
	@Mock
	MessageDrivenRunner mockRunner;
	@Mock
	ProgressCallback mockProgressCallback;
	@Mock
	HasQueueUrl mockHasQueueUrl;
	PollingMessageReceiverConfiguration config;
	String queueUrl;
	int messageVisibilityTimeoutSec;
	int semaphoreLockTimeoutSec;

	Message message;

	@Before
	public void before() {
		MockitoAnnotations.initMocks(this);
		queueUrl = "aQueueUrl";
		messageVisibilityTimeoutSec = 60;
		semaphoreLockTimeoutSec = 60;


		when(mockHasQueueUrl.getQueueUrl()).thenReturn(queueUrl);

		// setup for a single message.
		ReceiveMessageResult results = new ReceiveMessageResult();
		message = new Message();
		message.setReceiptHandle("handle");
		results.setMessages(Arrays.asList(message));
		ReceiveMessageResult emptyResults = new ReceiveMessageResult();
		emptyResults.setMessages(new LinkedList<Message>());
		when(
				mockAmazonSQSClient
						.receiveMessage(any(ReceiveMessageRequest.class)))
				.thenReturn(results, emptyResults);

		config = new PollingMessageReceiverConfiguration();
		config.setHasQueueUrl(mockHasQueueUrl);
		config.setRunner(mockRunner);
		config.setMessageVisibilityTimeoutSec(messageVisibilityTimeoutSec);
		config.setSemaphoreLockTimeoutSec(semaphoreLockTimeoutSec);

		when(mockProgressCallback.runnerShouldTerminate()).thenReturn(true);
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
		verify(mockRunner, times(1)).run(any(ProgressCallback.class),
				any(Message.class));
		// The message should be deleted
		DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
		deleteMessageRequest.setQueueUrl(queueUrl);
		deleteMessageRequest.setReceiptHandle(message.getReceiptHandle());
		verify(mockAmazonSQSClient, times(1)).deleteMessage(
				deleteMessageRequest);
		verify(mockProgressCallback).addProgressListener(any(ProgressListener.class));
		verify(mockProgressCallback).removeProgressListener(any(ProgressListener.class));
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
		verify(mockProgressCallback).addProgressListener(any(ProgressListener.class));
		verify(mockProgressCallback).removeProgressListener(any(ProgressListener.class));
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
		verify(mockProgressCallback).addProgressListener(any(ProgressListener.class));
		verify(mockProgressCallback).removeProgressListener(any(ProgressListener.class));
	}

	@Test
	public void testRunnerShouldTerminate() throws Exception {
		PollingMessageReceiverImpl receiver = new PollingMessageReceiverImpl(
				mockAmazonSQSClient, config);

		//always return a message
		ReceiveMessageResult results = new ReceiveMessageResult();
		Message message = new Message();
		results.setMessages(Collections.singletonList(message));
		when(mockAmazonSQSClient.receiveMessage(any(ReceiveMessageRequest.class))).thenAnswer((invocationOnMock) -> results);

		//should not terminate for 3 times that we check.
		when(mockProgressCallback.runnerShouldTerminate()).thenReturn(false, false, false, true);

		// call under test
		receiver.run(mockProgressCallback);

		//since receiver.run() uses a do/while loop. we've done work for numInvocations("progressCallback.runnerShouldTerminate()") + 1 times
		verify(mockRunner, times(4)).run(any(ProgressCallback.class),
				any(Message.class));
		verify(mockAmazonSQSClient, times(4)).deleteMessage(any(DeleteMessageRequest.class));
		verify(mockProgressCallback, times(4)).addProgressListener(any(ProgressListener.class));
		verify(mockProgressCallback, times(4)).removeProgressListener(any(ProgressListener.class));
	}
	
	@Test
	public void testDeleteOnShutdown() throws Throwable {
		PollingMessageReceiverImpl receiver = new PollingMessageReceiverImpl(
				mockAmazonSQSClient, config);

		// Simulate a JVM shutdown.
		receiver.forceShutdown();
		// call under test
		receiver.run(mockProgressCallback);
		verify(mockRunner, times(1)).run(any(ProgressCallback.class),
				any(Message.class));
		// The message should not be deleted after a shutdown.
		verify(mockAmazonSQSClient, never()).deleteMessage(any());
		verify(mockProgressCallback).addProgressListener(any(ProgressListener.class));
		verify(mockProgressCallback).removeProgressListener(any(ProgressListener.class));
	}
	
}
