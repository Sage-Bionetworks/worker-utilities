package org.sagebionetworks.workers.util.aws.message;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;

@RunWith(MockitoJUnitRunner.class)
public class MessageQueueImplTest {

	@Mock
	SqsClient mockSQSClient;

	String queueUrl;
	MessageQueueConfiguration config;

	@Before
	public void setUp() throws Exception {
		queueUrl = "queueURL";

		// config
		config = new MessageQueueConfiguration();
		config.setQueueName("queueName");
		config.setEnabled(true);

		when(mockSQSClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().queueUrl(queueUrl).build());
	}

	@Test
	public void testConstructor() {
		config = new MessageQueueConfiguration();
		String queueName = "queueName";
		config.setQueueName(queueName);
		config.setEnabled(true);
		MessageQueueImpl msgQImpl = new MessageQueueImpl(mockSQSClient, config);
		assertEquals(queueUrl, msgQImpl.getQueueUrl());
		verify(mockSQSClient).getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
		verifyNoMoreInteractions(mockSQSClient);
	}
}
