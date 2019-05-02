package org.sagebionetworks.workers.util.aws.message;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.sagebionetworks.common.util.progress.ProgressCallback;
import org.sagebionetworks.database.semaphore.CountingSemaphore;
import org.sagebionetworks.database.semaphore.LockKeyNotFoundException;
import org.sagebionetworks.workers.util.Gate;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;


@RunWith(MockitoJUnitRunner.class)
public class MessageDrivenWorkerStackTest {

	@Mock
	SqsClient mockSQSClient;
	@Mock
	CountingSemaphore mockSemaphore;
	@Mock
	Gate mockGate;
	@Mock
	MessageDrivenRunner mockRunner;

	String queueUrl;
	String queueArn;
	String topicArn;
	String token;
	Message message;
	MessageDrivenWorkerStackConfiguration config;
	
	int timeoutMS = 4000;

	@Before
	public void setUp() throws Exception {
		// mock queue
		queueUrl = "queueURL";
		queueArn = "queueArn";
		topicArn = "topicArn";

		// mock semaphore
		token = "aToken";
		when(mockSemaphore.attemptToAcquireLock(any(String.class),
						anyLong(), anyInt())).thenReturn(token);

		doThrow(LockKeyNotFoundException.class).when(mockSemaphore)
				.refreshLockTimeout(anyString(),anyString(), anyLong());

		// open by default
		when(mockGate.canRun()).thenReturn(true);

		// mock message receiver
		message = Message.builder()
				.receiptHandle("handle").build();
		ReceiveMessageResponse results = ReceiveMessageResponse.builder()
				.messages(Collections.singletonList(message)).build();

		ReceiveMessageResponse emptyResults = ReceiveMessageResponse.builder()
				.messages(Collections.emptyList()).build();
		when(mockSQSClient.receiveMessage(any(ReceiveMessageRequest.class)))
				.thenReturn(results, emptyResults);

		// default config
		config = new MessageDrivenWorkerStackConfiguration();
		config.setQueueName("queueName");
		config.setGate(mockGate);
		config.setRunner(mockRunner);
		config.setSemaphoreLockAndMessageVisibilityTimeoutSec(timeoutMS/1000);
		config.setSemaphoreLockKey("lockKey");
		config.setSemaphoreMaxLockCount(2);

		// mock the runner to call progressMade.
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				ProgressCallback callback = (ProgressCallback) invocation
						.getArguments()[0];
				Message message = (Message) invocation.getArguments()[1];
				// Wait for the timeout
				Thread.sleep(timeoutMS+100);
				return null;
			}
		}).when(mockRunner)
				.run(any(ProgressCallback.class), any(Message.class));

		when(mockSQSClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().queueUrl(queueUrl).build());

		//make sure we don't sit in a infinite while loop in PollingMessageReceiverImpl
		doThrow(LockKeyNotFoundException.class).when(mockSemaphore).refreshLockTimeout(anyString(),anyString(), anyLong());
	}

	
	@Test
	public void testProgressHeartbeatEnabled() throws RecoverableMessageException, Exception {
		// enable heartbeat.
		MessageDrivenWorkerStack stack = new MessageDrivenWorkerStack(
				mockSemaphore, mockSQSClient, config);

		doNothing().doNothing().doNothing().doThrow(LockKeyNotFoundException.class).when(mockSemaphore).refreshLockTimeout(anyString(),anyString(), anyLong());

		// setup the runner to just sleep with no progress
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				// Wait for the timeout
				Thread.sleep(timeoutMS*2);
				return null;
			}
		}).when(mockRunner)
				.run(any(ProgressCallback.class), any(Message.class));
		
		// call under test
		stack.run();
		// The progress should refresh the lock timeout.
		verify(mockSQSClient, atLeast(2)).changeMessageVisibility(any(ChangeMessageVisibilityRequest.class));
		verify(mockRunner).run(any(ProgressCallback.class), any(Message.class));
	}
	
	@Test
	public void testProgressHeartbeatDisabled() throws RecoverableMessageException, Exception {
		// disable heartbeat.
		MessageDrivenWorkerStack stack = new MessageDrivenWorkerStack(
				mockSemaphore, mockSQSClient, config);
		
		// setup the runner to just sleep with no progress
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				// Wait for the timeout
				Thread.sleep(timeoutMS*2);
				return null;
			}
		}).when(mockRunner)
				.run(any(ProgressCallback.class), any(Message.class));
		
		// call under test
		stack.run();
		// The progress should refresh the lock timeout.
		verify(mockSQSClient, never()).changeMessageVisibility(any(ChangeMessageVisibilityRequest.class));
		verify(mockRunner).run(any(ProgressCallback.class), any(Message.class));
	}

}
