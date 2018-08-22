package org.sagebionetworks.workers.util.aws.message;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.ComparisonOperator;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.PutMetricAlarmRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.cloudwatch.model.Statistic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

/**
 * Provides information about an AWS SQS queue. The constructor will create a
 * queue with the configured name if the queue does not already exist. The ARN
 * and URL of the queue will also be cached for the queue.
 * 
 * If the provided configuration information includes a dead letter queue name
 * and max failure count, then queue will be configured with the dead letter
 * queue.
 * 
 * Additionally, if the configuration includes a list of AWS SNS topic names the
 * queue will be configured to receive messages from each topic. This includes
 * creating each topic if it does not exist, configuring the policy of the queue
 * such that the topic has permission to push messages to the queue and subscribing the
 * queue to the topic.
 */
public class MessageQueueImpl implements MessageQueue {

	public static final String VISIBILITY_TIMEOUT_KEY = "VisibilityTimeout";

	public static final String POLICY_KEY = "Policy";

	public static final String REDRIVE_POLICY_KEY = "RedrivePolicy";

	public static final String PROTOCOL_SQS = "sqs";

	public static final String QUEUE_ARN_KEY = "QueueArn";

	private Logger logger = LogManager.getLogger(MessageQueueImpl.class);

	// The first argument is the ARN of the queue, and the second is the ARN of the topic.
	public static final String GRAN_SET_MESSAGE_TEMPLATE = "{ \"Id\":\"GrantRepoTopicSendMessage\", \"Statement\": [{ \"Sid\":\"1\",  \"Resource\": \"%1$s\", \"Effect\": \"Allow\", \"Action\": \"SQS:SendMessage\", \"Condition\": {\"ArnEquals\": {\"aws:SourceArn\": %2$s}}, \"Principal\": {\"AWS\": \"*\"}}]}";

	private AmazonSQSClient awsSQSClient;

	private AmazonSNSClient awsSNSClient;

	private AmazonCloudWatch awsCloudWatchClient;

	private final String queueName;
	private final List<String> topicNamesToSubscribe;
	private String queueUrl;
	private boolean isEnabled;
	private final Integer maxFailureCount;
	private final String deadLetterQueueName;
	private String deadLetterQueueUrl;
	private final Integer defaultMessageVisibilityTimeoutSec;
	private final Integer oldestMessageInQueueAlarmThresholdSec;
	private final String alarmNotificationARN;

	/**
	 * @param awsSQSClient An AmazonSQSClient configured with credentials.
	 * @param awsSNSClient An AmazonSNSClient configured with credentials.
	 * @param config Configuration information for this queue.
	 */
	public MessageQueueImpl(AmazonSQSClient awsSQSClient,
			AmazonSNSClient awsSNSClient, AmazonCloudWatch awsCloudWatchClient, MessageQueueConfiguration config) {
		super();
		this.awsSQSClient = awsSQSClient;
		this.awsSNSClient = awsSNSClient;
		this.awsCloudWatchClient = awsCloudWatchClient;
		this.isEnabled = config.isEnabled();
		this.queueName = config.getQueueName();
		this.defaultMessageVisibilityTimeoutSec = config.getDefaultMessageVisibilityTimeoutSec();
		if (this.queueName == null) {
			throw new IllegalArgumentException("QueueName cannot be null");
		}
		if(this.defaultMessageVisibilityTimeoutSec == null){
			throw new IllegalArgumentException("DefaultMessageVisibilityTimeoutSec cannot be null");
		}
		this.topicNamesToSubscribe = config.getTopicNamesToSubscribe();
		this.deadLetterQueueName = config.getDeadLetterQueueName();
		this.maxFailureCount = config.getMaxFailureCount();
		this.deadLetterQueueUrl = null;
		if (! validateDeadLetterParams(deadLetterQueueName, maxFailureCount)) {
			throw new IllegalArgumentException("maxFailureCount and deadLetterQueueName must both be either null or not null");
		}
		this.oldestMessageInQueueAlarmThresholdSec = config.getOldestMessageInQueueAlarmThresholdSec();
		this.alarmNotificationARN = config.getOldestMessageInQueueAlarmNotificationARN();
		setup();
	}

	public MessageQueueImpl(AmazonSQSClient awsSQSClient,
							AmazonSNSClient awsSNSClient, MessageQueueConfiguration config){
		this(awsSQSClient, awsSNSClient, null, config);
	}

	/**
	 * The idempotent queue setup.
	 */
	private void setup() {
		// Do nothing if it is not enabled
		if(!isEnabled){
			logger.info("Queue: "+queueName+" will not be configured because it is not enabled");
			return;
		}

		// Create the queue if it does not already exist
		final String queueUrl = createQueue(queueName, defaultMessageVisibilityTimeoutSec);
		final String queueArn = getQueueArn(queueUrl);
		this.logger.info("Queue created. URL: " + queueUrl + " ARN: " + queueArn);

		//Add the alarm for the queue if necessary
		addWorkerQueueAlarmIfNecessary();

		// Create the dead letter queue as requested
		String dlqUrl = null;
		String dlqArn = null;
		if (deadLetterQueueName != null) {
			dlqUrl = createQueue(deadLetterQueueName, defaultMessageVisibilityTimeoutSec);
			dlqArn = getQueueArn(dlqUrl);
			this.logger.info("Queue created. URL: " + dlqUrl + " ARN: " + dlqArn);
			this.deadLetterQueueUrl = dlqUrl;
			grantRedrivePolicy(queueUrl, dlqArn, maxFailureCount);
		}

		// create topics and setup access.
		createAndGrandAccessToTopics(queueArn, queueUrl, this.topicNamesToSubscribe);

		this.queueUrl = queueUrl;
	}
	
	/**
	 * Create the queue if it does not exist and return the queue URL
	 * @param qName
	 * @return The URL fo the queue.
	 */
	protected String createQueue(String qName, Integer defaultMessageVisibilityTimeoutSec) {
		CreateQueueRequest cqRequest = new CreateQueueRequest(qName);
		CreateQueueResult cqResult = this.awsSQSClient.createQueue(cqRequest);
		String qUrl = cqResult.getQueueUrl();
		GetQueueAttributesResult atts = this.awsSQSClient.getQueueAttributes(qUrl, Arrays.asList(VISIBILITY_TIMEOUT_KEY));
		String expectedValue = defaultMessageVisibilityTimeoutSec.toString();
		if(!expectedValue.equals(atts.getAttributes().get(VISIBILITY_TIMEOUT_KEY))){
			// We need to set it
			this.awsSQSClient.setQueueAttributes(new SetQueueAttributesRequest().withQueueUrl(qUrl).addAttributesEntry(VISIBILITY_TIMEOUT_KEY, expectedValue));
		}
		return qUrl;
	}

	/**
	 * Lookup the ARN of the queue using the queus's URL.
	 * @param qUrl
	 * @return
	 */
	protected String getQueueArn(String qUrl) {
		String attrName = QUEUE_ARN_KEY;
		GetQueueAttributesRequest attrRequest = new GetQueueAttributesRequest()
				.withQueueUrl(qUrl)
				.withAttributeNames(attrName);
		GetQueueAttributesResult attrResult = this.awsSQSClient.getQueueAttributes(attrRequest);
		String qArn = attrResult.getAttributes().get(attrName);
		if (qArn == null) {
			throw new IllegalStateException("Failed to get the ARN for Queue URL: " + qUrl);
		}
		return qArn;
	}

	protected void addWorkerQueueAlarmIfNecessary(){
		if (this.oldestMessageInQueueAlarmThresholdSec == null
				|| this.alarmNotificationARN == null
				|| this.awsCloudWatchClient == null){
			return;
		}

		Dimension metricDimension = new Dimension().withName("QueueName").withValue(this.queueName);
		PutMetricAlarmRequest putMetricAlarmRequest = new PutMetricAlarmRequest();
		putMetricAlarmRequest
				.withNamespace("AWS/SQS")
				.withDimensions(metricDimension)
				.withAlarmName(this.queueName + "-oldest-message-exceed-time-alarm")
				.withAlarmDescription(String.format("Alarm when oldest message in the %s queue exceeds %d seconds",
						this.queueName, this.oldestMessageInQueueAlarmThresholdSec))

				.withMetricName("ApproximateAgeOfOldestMessage")
				.withStatistic(Statistic.Maximum)
				.withComparisonOperator(ComparisonOperator.GreaterThanOrEqualToThreshold)
				.withThreshold(this.oldestMessageInQueueAlarmThresholdSec.doubleValue())
				.withUnit(StandardUnit.Seconds)

				.withPeriod(300) //5 minutes
				.withEvaluationPeriods(1)

				.withActionsEnabled(true)
				.withAlarmActions(this.alarmNotificationARN);

		awsCloudWatchClient.putMetricAlarm(putMetricAlarmRequest);
	}
	
	/**
	 * Validate that both or neither parameter are set.
	 * @param deadLetterQueueName
	 * @param maxReceiveCount
	 * @return
	 */
	protected static boolean validateDeadLetterParams(String deadLetterQueueName, Integer maxReceiveCount) {
		if (deadLetterQueueName == null) {
			return (maxReceiveCount == null);
		} else {
			return (maxReceiveCount != null);
		}
	}

	@Override
	public String getQueueUrl() {
		return this.queueUrl;
	}

	@Override
	public String getQueueName(){
		return this.queueName;
	}
	
	/**
	 * Create each topic in the list (if needed), subscribe the queue to the topic,
	 * and grant access access to publish from the topics to the queue.
	 * 
	 * @param queueArn
	 * @param queueUrl
	 * @param topicsToSubscribe List of topic names to register this queue with.
	 */
	protected void createAndGrandAccessToTopics(String queueArn, String queueUrl, List<String> topicsToSubscribe) {
		if (topicsToSubscribe == null) {
			return;
		}
		List<String> topicArns = createTopics(topicsToSubscribe);
		for (String topicArn : topicArns) {
			final Subscription subscription = subscribeQueueToTopicIfNeeded(topicArn, queueArn);
			if (subscription == null) {
				throw new IllegalStateException("Failed to subscribe queue (" + queueArn + ") to topic (" + topicArn + ")");
			}
		}
		// Make sure the topics has the permission it needs
		grantPolicyIfNeeded(topicArns, queueArn, queueUrl);
	}
		
	/**
	 * Creates each topic if needed and returns the topic ARNs.
	 * 	
	 * @param topicsToSubscribe
	 * @return a list of topic ARNs
	 */
	private List<String> createTopics(List<String> topicsToSubscribe) {
		List<String> arns = new ArrayList<String>();
		for (String topicName : topicsToSubscribe) {
			CreateTopicRequest ctRequest = new CreateTopicRequest(topicName);
			CreateTopicResult topicResult = this.awsSNSClient.createTopic(ctRequest);
			final String topicArn = topicResult.getTopicArn();
			arns.add(topicArn);
		}
		return arns;
	}

	/**
	 * Subscribes this queue to the topic if needed.
	 * @param topicArn
	 * @param queueArn
	 * @return
	 */
	protected Subscription subscribeQueueToTopicIfNeeded(final String topicArn, final String queueArn) {
		logger.info("Subscribing " + queueArn + " to " + topicArn);
		if(topicArn == null){
			throw new IllegalArgumentException("topicArn cannot be null");
		}
		if(queueArn == null){
			throw new IllegalArgumentException("queueArn cannot be null");
		}
		Subscription sub = findSubscription(topicArn, queueArn);
		if (sub != null) {
			return sub;
		}
		// We did not find the subscription so create it
		SubscribeRequest request = new SubscribeRequest(topicArn, PROTOCOL_SQS, queueArn);
		this.awsSNSClient.subscribe(request);
		return findSubscription(topicArn, queueArn);
	}

	/**
	 * Finds this subscription of the topic to the queue if it exists.
	 * @param topicArn
	 * @param queueArn
	 * @return
	 */
	protected Subscription findSubscription(final String topicArn, final String queueArn) {
		if(topicArn == null){
			throw new IllegalArgumentException("topicArn cannot be null");
		}
		if(queueArn == null){
			throw new IllegalArgumentException("queueArn cannot be null");
		}
		ListSubscriptionsByTopicResult result;
		do {
			// Keep looking until we find it or run out of nextTokens.
			ListSubscriptionsByTopicRequest request = new ListSubscriptionsByTopicRequest(topicArn);
			result = this.awsSNSClient.listSubscriptionsByTopic(request);
			for (Subscription subscription : result.getSubscriptions()) {
				if (subscription.getProtocol().equals(PROTOCOL_SQS) &&
						subscription.getEndpoint().equals(queueArn) && 
						subscription.getTopicArn().equals(topicArn)) {
					return subscription;
				}
			}
		} while (result.getNextToken() != null);
		return null;
	}
	
	/**
	 * Grant the topic permission to write to the queue if it does not already
	 * have such a permission.
	 * 
	 * @param topicsToSubscribe
	 * @param queueArn
	 * @param queueUrl
	 */
	protected void grantPolicyIfNeeded(final List<String> topicArns, final String queueArn, final String queueUrl) {
		if(topicArns == null){
			throw new IllegalArgumentException("topicArn cannot be null");
		}
		if(queueArn == null){
			throw new IllegalArgumentException("queueArn cannot be null");
		}
		GetQueueAttributesRequest attrRequest = new GetQueueAttributesRequest()
				.withQueueUrl(queueUrl)
				.withAttributeNames(POLICY_KEY);
		GetQueueAttributesResult attrResult = this.awsSQSClient.getQueueAttributes(attrRequest);
		String policyString =  attrResult.getAttributes().get(POLICY_KEY);
		this.logger.info("Currently policy: " + policyString);
		if (policyString == null || !TopicUtils.containsAllTopics(policyString, topicArns)) {
			this.logger.info("Policy not set to grant the topic write permission to the queue. Adding a policy now...");
			// Now we need to grant the topic permission to send messages to the queue.
			String permissionString = createGrantPolicyTopicToQueueString(queueArn, TopicUtils.generateSourceArn(topicArns));
			Map<String, String> map = new HashMap<String, String>();
			map.put(POLICY_KEY, permissionString);
			this.logger.info("Setting policy to: "+permissionString);
			SetQueueAttributesRequest setAttrRequest = new SetQueueAttributesRequest()
					.withQueueUrl(queueUrl)
					.withAttributes(map);
			this.awsSQSClient.setQueueAttributes(setAttrRequest);
		} else {
			this.logger.info("Topic already has sendMessage permission on this queue");
		}
	}

	/**
	 * The JSON policy string used to grant a topic permission to forward messages to to a queue.
	 * @param queueArn
	 * @param arnToGrant
	 * @return
	 */
	public static String createGrantPolicyTopicToQueueString(final String queueArn,
			String arnToGrant) {
		return String.format(GRAN_SET_MESSAGE_TEMPLATE, queueArn, arnToGrant);
	}
	
	/**
	 * Grant the required permission to push messages the dead letter queue.
	 * 
	 * @param queueUrl
	 * @param deadLetterQueueArn
	 * @param maxReceiveCount
	 */
	protected void grantRedrivePolicy(String queueUrl, String deadLetterQueueArn, Integer maxReceiveCount) {
		GetQueueAttributesRequest attrRequest = new GetQueueAttributesRequest()
			.withQueueUrl(queueUrl)
			.withAttributeNames(REDRIVE_POLICY_KEY);
		GetQueueAttributesResult attrResult = this.awsSQSClient.getQueueAttributes(attrRequest);
		String ps =  attrResult.getAttributes().get(REDRIVE_POLICY_KEY);
		this.logger.info("Current redrive policy: " + ps);
		if (ps == null || ps.indexOf(deadLetterQueueArn) < 1) {
			String redrivePolicy = getRedrivePolicy(maxReceiveCount, deadLetterQueueArn);
			SetQueueAttributesRequest queueAttributes = new SetQueueAttributesRequest();
			Map<String,String> attributes = new HashMap<String,String>();            
			attributes.put(REDRIVE_POLICY_KEY, redrivePolicy);            
			queueAttributes.setAttributes(attributes);
			queueAttributes.setQueueUrl(queueUrl);
			this.awsSQSClient.setQueueAttributes(queueAttributes);
		} else {
			this.logger.info("Dead letter queue already configured on this queue");
		}
	}

	/**
	 * Create the RedrivePolicy JSON for setting up a dead letter queue.
	 * @param maxReceiveCount
	 * @param deadLetterQueArn
	 * @return
	 */
	protected String getRedrivePolicy(Integer maxReceiveCount, String deadLetterQueArn) {
		return(String.format("{\"maxReceiveCount\":\"%d\", \"deadLetterTargetArn\":\"%s\"}", maxReceiveCount, deadLetterQueArn));
	}
	
	@Override
	public boolean isEnabled() {
		return isEnabled;
	}
	
	@Override
	public String getDeadLetterQueueName() {
		return this.deadLetterQueueName;
	}
	
	@Override
	public Integer getMaxReceiveCount() {
		return this.maxFailureCount;
	}
	
	@Override
	public String getDeadLetterQueueUrl() {
		return this.deadLetterQueueUrl;
	}
}
