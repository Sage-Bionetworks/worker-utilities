package org.sagebionetworks.workers.util.aws.message;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.matchers.Any;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.*;

import org.sagebionetworks.database.semaphore.CountingSemaphore;
import org.sagebionetworks.workers.util.Gate;
import org.sagebionetworks.workers.util.progress.ProgressCallback;

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

public class MessageDrivenWorkerStackTest {

	AmazonSQSClient mockSQSClient;
	AmazonSNSClient mockSNSClient;
	CountingSemaphore mockSemaphore;
	Gate mockGate;
	MessageDrivenRunner mockRunner;

	String queueUrl;
	String queueArn;
	String topicArn;
	String token;
	Message message;
	MessageDrivenWorkerStackConfiguration config;

	@Before
	public void setUp() throws Exception {
		mockSQSClient = Mockito.mock(AmazonSQSClient.class);
		mockSNSClient = Mockito.mock(AmazonSNSClient.class);
		mockSemaphore = Mockito.mock(CountingSemaphore.class);
		mockRunner = Mockito.mock(MessageDrivenRunner.class);
		mockGate = Mockito.mock(Gate.class);

		// mock queue
		queueUrl = "queueURL";
		queueArn = "queueArn";
		topicArn = "topicArn";
		CreateQueueResult expectedRes = new CreateQueueResult()
				.withQueueUrl(queueUrl);
		when(mockSQSClient.createQueue(any(CreateQueueRequest.class)))
				.thenReturn(expectedRes);
		Map<String,String> attMap = new HashMap<String, String>(1);
		attMap.put(MessageQueueImpl.QUEUE_ARN_KEY, queueArn);
		GetQueueAttributesResult queAttributeResults = new GetQueueAttributesResult();
		queAttributeResults.setAttributes(attMap);
		when(mockSQSClient.getQueueAttributes(any(GetQueueAttributesRequest.class))).thenReturn(queAttributeResults);
		CreateTopicResult createTopicResults = new CreateTopicResult();
		createTopicResults.setTopicArn(topicArn);
		when(mockSNSClient.createTopic(any(CreateTopicRequest.class))).thenReturn(createTopicResults);
		ListSubscriptionsByTopicResult listSubscriptionResults = new ListSubscriptionsByTopicResult();
		listSubscriptionResults.setNextToken(null);
		Subscription subscription = new Subscription();
		subscription.setTopicArn(topicArn);
		subscription.setEndpoint(queueArn);
		subscription.setProtocol(MessageQueueImpl.PROTOCOL_SQS);
		listSubscriptionResults.setSubscriptions(Arrays.asList(subscription));
		when(mockSNSClient.listSubscriptionsByTopic(any(ListSubscriptionsByTopicRequest.class))).thenReturn(listSubscriptionResults);
		
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
		config.setSemaphoreLockAndMessageVisibilityTimeoutSec(60);
		config.setSemaphoreLockKey("lockKey");
		config.setSemaphoreMaxLockCount(2);
	}

	@Test
	public void testHappyRun() throws RecoverableMessageException, Exception {
		MessageDrivenWorkerStack stack = new MessageDrivenWorkerStack(
				mockSemaphore, mockSQSClient, mockSNSClient, config);
		// call under test
		stack.run();
		// happy case a message should be passed to the runner.
		verify(mockRunner).run(any(ProgressCallback.class), any(Message.class));
	}
	
	@Test
	public void testRunGateFalse() throws RecoverableMessageException, Exception {
		when(mockGate.canRun()).thenReturn(false);
		MessageDrivenWorkerStack stack = new MessageDrivenWorkerStack(
				mockSemaphore, mockSQSClient, mockSNSClient, config);
		// call under test
		stack.run();
		verify(mockRunner, never()).run(any(ProgressCallback.class), any(Message.class));
	}
	
	@Test
	public void testHappyNullGate() throws RecoverableMessageException, Exception {
		// gate is not required
		config.setGate(null);
		MessageDrivenWorkerStack stack = new MessageDrivenWorkerStack(
				mockSemaphore, mockSQSClient, mockSNSClient, config);
		// call under test
		stack.run();
		verify(mockRunner).run(any(ProgressCallback.class), any(Message.class));
	}
	
	@Test
	public void testProgressInStack() throws RecoverableMessageException, Exception {
		MessageDrivenWorkerStack stack = new MessageDrivenWorkerStack(
				mockSemaphore, mockSQSClient, mockSNSClient, config);
		
		// mock the runner to call progressMade.
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				ProgressCallback<Message> callback = (ProgressCallback<Message>) invocation
						.getArguments()[0];
				Message message = (Message) invocation.getArguments()[1];

				// call back
				callback.progressMade(message);
				return null;
			}
		}).when(mockRunner)
				.run(any(ProgressCallback.class), any(Message.class));
		
		// call under test
		stack.run();
		// The progress should refresh the lock timeout.
		verify(mockSemaphore).refreshLockTimeout(anyString(), anyString(), anyLong());
		// The progress should refresh the visibility.
		verify(mockSQSClient).changeMessageVisibility(any(ChangeMessageVisibilityRequest.class));
		
		verify(mockRunner).run(any(ProgressCallback.class), any(Message.class));
	}
	
	@Test
	public void testQueueWithTopic(){
		config.setTopicNamesToSubscribe(Arrays.asList("SomeTopicName"));
		MessageDrivenWorkerStack stack = new MessageDrivenWorkerStack(
				mockSemaphore, mockSQSClient, mockSNSClient, config);
		// the topic should be created if needed.
		verify(mockSNSClient).createTopic(any(CreateTopicRequest.class));
	}
	
	@Test
	public void testSetDeadLetter(){
		config.setDeadLetterQueueName("deadletters");
		config.setDeadLetterMaxFailureCount(5);
		MessageDrivenWorkerStack stack = new MessageDrivenWorkerStack(
				mockSemaphore, mockSQSClient, mockSNSClient, config);
		// the topic should be created if needed.
		verify(mockSQSClient).createQueue(new CreateQueueRequest("deadletters"));	
	}

}
