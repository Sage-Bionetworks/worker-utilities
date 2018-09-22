package org.sagebionetworks.workers.util.aws.message;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import com.amazonaws.services.sqs.model.GetQueueUrlResult;
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
import org.sagebionetworks.workers.util.Gate;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicRequest;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicResult;
import com.amazonaws.services.sns.model.Subscription;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

@RunWith(MockitoJUnitRunner.class)
public class MessageDrivenWorkerStackTest {

	@Mock
	AmazonSQSClient mockSQSClient;
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
		// open by default
		when(mockGate.canRun()).thenReturn(true);

		// mock message receiver
		ReceiveMessageResult results = new ReceiveMessageResult();
		message = new Message();
		message.setReceiptHandle("handle");
		results.setMessages(Arrays.asList(message));
		ReceiveMessageResult emptyResults = new ReceiveMessageResult();
		emptyResults.setMessages(new LinkedList<Message>());
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

		when(mockSQSClient.getQueueUrl(anyString())).thenReturn(new GetQueueUrlResult().withQueueUrl(queueUrl));
	}

	@Test
	public void testHappyRun() throws RecoverableMessageException, Exception {
		MessageDrivenWorkerStack stack = new MessageDrivenWorkerStack(
				mockSemaphore, mockSQSClient, config);
		// call under test
		stack.run();
		// happy case a message should be passed to the runner.
		verify(mockRunner).run(any(ProgressCallback.class), any(Message.class));
	}
	
	@Test
	public void testRunGateFalse() throws RecoverableMessageException, Exception {
		when(mockGate.canRun()).thenReturn(false);
		MessageDrivenWorkerStack stack = new MessageDrivenWorkerStack(
				mockSemaphore, mockSQSClient, config);
		// call under test
		stack.run();
		verify(mockRunner, never()).run(any(ProgressCallback.class), any(Message.class));
	}
	
	@Test
	public void testHappyNullGate() throws RecoverableMessageException, Exception {
		// gate is not required
		config.setGate(null);
		MessageDrivenWorkerStack stack = new MessageDrivenWorkerStack(
				mockSemaphore, mockSQSClient, config);
		// call under test
		stack.run();
		verify(mockRunner).run(any(ProgressCallback.class), any(Message.class));
	}
	
	@Test
	public void testProgressHeartbeatEnabled() throws RecoverableMessageException, Exception {
		// enable heartbeat.
		config.setUseProgressHeartbeat(true);
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
		verify(mockSQSClient, atLeast(2)).changeMessageVisibility(any(ChangeMessageVisibilityRequest.class));
		verify(mockRunner).run(any(ProgressCallback.class), any(Message.class));
	}
	
	@Test
	public void testProgressHeartbeatDisabled() throws RecoverableMessageException, Exception {
		// disable heartbeat.
		config.setUseProgressHeartbeat(false);
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
