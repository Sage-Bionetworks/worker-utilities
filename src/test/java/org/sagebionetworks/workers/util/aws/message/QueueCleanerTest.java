package org.sagebionetworks.workers.util.aws.message;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

@RunWith(MockitoJUnitRunner.class)
public class QueueCleanerTest {
	@Mock
	SqsClient mockAmazonSQSClient;
	String queueUrl;
	QueueCleaner queueCleaner;

	@Before
	public void before() {
		queueUrl = "aQueueUrl";

		when(mockAmazonSQSClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(
				GetQueueUrlResponse.builder().queueUrl(queueUrl).build());

		// Simulate two batches to delete
		ReceiveMessageResponse pageOne = ReceiveMessageResponse.builder()
				.messages(Arrays.asList(
						Message.builder().messageId("id1").receiptHandle("h1").build(),
						Message.builder().messageId("id2").receiptHandle("h2").build()))
				.build();
		ReceiveMessageResponse pageTwo = ReceiveMessageResponse.builder()
				.messages(Collections.singletonList(Message.builder().messageId("id3").receiptHandle("h3").build()))
				.build();
		// page three is empty.
		ReceiveMessageResponse pageThree = ReceiveMessageResponse.builder().messages(Collections.emptyList()).build();
		when(
				mockAmazonSQSClient
						.receiveMessage(any(ReceiveMessageRequest.class)))
				.thenReturn(pageOne, pageTwo, pageThree);
		
		queueCleaner = new QueueCleaner(mockAmazonSQSClient);
	}

	@Test
	public void testAttemptToEmptyQueue() {
		// Simulate two batches to delete
		queueCleaner.purgeQueue("someQueue");
		// each page should be deleted as a batch.
		verify(mockAmazonSQSClient, times(2)).deleteMessageBatch(
				any(DeleteMessageBatchRequest.class));
	}
	
	@Test
	public void testQueueDoesNotExist(){
		when(mockAmazonSQSClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenThrow(QueueDoesNotExistException.builder().message("Not found").build());
		// should not fail.
		queueCleaner.purgeQueue("someQueue");
	}
}
