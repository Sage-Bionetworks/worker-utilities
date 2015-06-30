package org.sagebionetworks.workers.util.aws.message;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicRequest;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicResult;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.Subscription;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;

import static org.sagebionetworks.workers.util.aws.message.MessageQueueImpl.*;

public class MessageQueueImplTest {
	
	AmazonSQSClient mockSQSClient;
	AmazonSNSClient mockSNSClient;
	String queueUrl;
	String queueArn;
	String topicArn;
	MessageQueueConfiguration config;

	@Before
	public void setUp() throws Exception {
		mockSQSClient = Mockito.mock(AmazonSQSClient.class);
		mockSNSClient = Mockito.mock(AmazonSNSClient.class);
		queueUrl = "queueURL";
		queueArn = "queueArn";
		topicArn = "topicArn";
		CreateQueueResult expectedRes = new CreateQueueResult().withQueueUrl(queueUrl);
		when(mockSQSClient.createQueue(any(CreateQueueRequest.class))).thenReturn(expectedRes);
		GetQueueAttributesResult queAttributeResults = new GetQueueAttributesResult();
		Map<String,String> attMap = new HashMap<String, String>(1);
		attMap.put(MessageQueueImpl.QUEUE_ARN_KEY, queueArn);
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
		
		// config
		config = new MessageQueueConfiguration();
		config.setTopicNamesToSubscribe(Arrays.asList("testTopic"));
		config.setQueueName("queueName");
		config.setEnabled(true);
		config.setDeadLetterQueueName("deadLetterQueueName");
		config.setMaxFailureCount(5);
	}

	@Test
	public void testCreateQueueOnly() {
		config = new MessageQueueConfiguration();
		String queueName = "queueName";
		config.setQueueName(queueName);
		config.setEnabled(true);
		MessageQueueImpl msgQImpl = new MessageQueueImpl(mockSQSClient, mockSNSClient, config);
		assertEquals(queueUrl, msgQImpl.getQueueUrl());
	}
	
	@Test
	public void testGetQueueArn() {
		MessageQueueImpl msgQImpl = new MessageQueueImpl(mockSQSClient, mockSNSClient, config);
		String qArn = msgQImpl.getQueueArn(queueUrl);
		assertEquals(queueArn, qArn);
	}
	
	@Test
	public void testCreateAndGrantAccessToTopic(){
		MessageQueueImpl msgQImpl = new MessageQueueImpl(mockSQSClient, mockSNSClient, config);
		
		// mockSQSClient.setQueueAttributes() is called twice while the msgQImpl is initiated
		verify(mockSQSClient, times(2)).setQueueAttributes((SetQueueAttributesRequest) any());
		
		reset(mockSNSClient);
		String newTopicName = "newTopicName";
		String newTopicArn = "newTopicArn";
		// creates the topic an fetches the topic ARN.
		CreateTopicResult createTopicResults = new CreateTopicResult();
		createTopicResults.setTopicArn(newTopicArn);
		when(mockSNSClient.createTopic(any(CreateTopicRequest.class))).thenReturn(createTopicResults);
		
		// the first subscription source does not include the new topic.
		ListSubscriptionsByTopicResult fristSubscriptionResutls = new ListSubscriptionsByTopicResult();
		fristSubscriptionResutls.setNextToken(null);
		Subscription otherSubscription = new Subscription();
		otherSubscription.setTopicArn(topicArn);
		otherSubscription.setEndpoint(queueArn);
		otherSubscription.setProtocol(PROTOCOL_SQS);
		fristSubscriptionResutls.setSubscriptions(Arrays.asList(otherSubscription));
		// The second time the subscription should exist.
		ListSubscriptionsByTopicResult secondSubscriptionResutls = new ListSubscriptionsByTopicResult();
		secondSubscriptionResutls.setNextToken(null);
		Subscription newSubscription = new Subscription();
		newSubscription.setTopicArn(newTopicArn);
		newSubscription.setEndpoint(queueArn);
		newSubscription.setProtocol(PROTOCOL_SQS);
		secondSubscriptionResutls.setSubscriptions(Arrays.asList(otherSubscription, newSubscription));
		// configure both results.
		when(mockSNSClient.listSubscriptionsByTopic(any(ListSubscriptionsByTopicRequest.class))).thenReturn(fristSubscriptionResutls, secondSubscriptionResutls);

		// call under test.
		msgQImpl.createAndGrandAccessToTopics(queueArn, queueUrl, Arrays.asList(newTopicName));
		SubscribeRequest request = new SubscribeRequest(newTopicArn, PROTOCOL_SQS, queueArn);
		verify(mockSNSClient).subscribe(request);
		
		// verify the the policy is set.
		String permissionString = MessageQueueImpl.createGrantPolicyTopicToQueueString(queueArn, newTopicArn);
		Map<String, String> map = new HashMap<String, String>();
		map.put(POLICY_KEY, permissionString);
		SetQueueAttributesRequest setAttrRequest = new SetQueueAttributesRequest()
				.withQueueUrl(queueUrl)
				.withAttributes(map);
		verify(mockSQSClient).setQueueAttributes(setAttrRequest);
	}
	
	@Test
	public void testGetRedrivePolicy() {
		MessageQueueImpl msgQImpl = new MessageQueueImpl(mockSQSClient, mockSNSClient, config);
		String expectedPolicy = "{\"maxReceiveCount\":\"5\", \"deadLetterTargetArn\":\"deadLetterQueueArn\"}";
		String s = msgQImpl.getRedrivePolicy(5, "deadLetterQueueArn");
		assertEquals(expectedPolicy, s);
	}
	
	@Test
	public void testValidateDeadLetterParams() {
		String dlqName = "deadLetterQ";
		Integer mrc = 1;
		assertEquals(true, MessageQueueImpl.validateDeadLetterParams(dlqName, mrc));
		mrc = null;
		assertEquals(false, MessageQueueImpl.validateDeadLetterParams(dlqName, mrc));
		dlqName = null;
		assertEquals(true, MessageQueueImpl.validateDeadLetterParams(dlqName, mrc));
		mrc = 1;
		assertEquals(false, MessageQueueImpl.validateDeadLetterParams(dlqName, mrc));
	}
	
}
