/*
 * Copyright 2010-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.sqs.javamessaging;

import java.util.*;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.util.StringUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.payloadoffloading.*;


/**
 * Amazon SQS Extended Client extends the functionality of Amazon SQS client.
 * All service calls made using this client are blocking, and will not return
 * until the service call completes.
 *
 * <p>
 * The Amazon SQS extended client enables sending and receiving large messages
 * via Amazon S3. You can use this library to:
 * </p>
 *
 * <ul>
 * <li>Specify whether messages are always stored in Amazon S3 or only when a
 * message size exceeds 256 KB.</li>
 * <li>Send a message that references a single message object stored in an
 * Amazon S3 bucket.</li>
 * <li>Get the corresponding message object from an Amazon S3 bucket.</li>
 * <li>Delete the corresponding message object from an Amazon S3 bucket.</li>
 * </ul>
 */
public class AmazonSQSExtendedClient extends AmazonSQSExtendedClientBase {
    static final String USER_AGENT_HEADER = Util.getUserAgentHeader(AmazonSQSExtendedClient.class.getSimpleName());
    static final String USER_AGENT_HEADER_NAME = "User-Agent";

    private static final Log LOG = LogFactory.getLog(AmazonSQSExtendedClient.class);
    static final String LEGACY_RESERVED_ATTRIBUTE_NAME = "SQSLargePayloadSize";
    static final List<String> RESERVED_ATTRIBUTE_NAMES = Arrays.asList(LEGACY_RESERVED_ATTRIBUTE_NAME,
            SQSExtendedClientConstants.RESERVED_ATTRIBUTE_NAME);
    private ExtendedClientConfiguration clientConfiguration;
    private PayloadStore payloadStore;

    /**
     * Constructs a new Amazon SQS extended client to invoke service methods on
     * Amazon SQS with extended functionality using the specified Amazon SQS
     * client object.
     *
     * <p>
     * All service calls made using this new client object are blocking, and
     * will not return until the service call completes.
     *
     * @param sqsClient
     *            The Amazon SQS client to use to connect to Amazon SQS.
     */
    public AmazonSQSExtendedClient(SqsClient sqsClient) {
        this(sqsClient, new ExtendedClientConfiguration());
    }

    /**
     * Constructs a new Amazon SQS extended client to invoke service methods on
     * Amazon SQS with extended functionality using the specified Amazon SQS
     * client object.
     *
     * <p>
     * All service calls made using this new client object are blocking, and
     * will not return until the service call completes.
     *
     * @param sqsClient
     *            The Amazon SQS client to use to connect to Amazon SQS.
     * @param extendedClientConfig
     *            The extended client configuration options controlling the
     *            functionality of this client.
     */
    public AmazonSQSExtendedClient(SqsClient sqsClient, ExtendedClientConfiguration extendedClientConfig) {
        super(sqsClient);
        this.clientConfiguration = new ExtendedClientConfiguration(extendedClientConfig);
        S3Dao s3Dao = new S3Dao(clientConfiguration.getAmazonS3Client());
        this.payloadStore = new S3BackedPayloadStore(s3Dao, clientConfiguration.getS3BucketName(),
                clientConfiguration.getSSEAwsKeyManagementParams());
    }

    /**
     * <p>
     * Delivers a message to the specified queue and uploads the message payload
     * to Amazon S3 if necessary.
     * </p>
     * <p>
     * <b>IMPORTANT:</b> The following list shows the characters (in Unicode)
     * allowed in your message, according to the W3C XML specification. For more
     * information, go to http://www.w3.org/TR/REC-xml/#charsets If you send any
     * characters not included in the list, your request will be rejected. #x9 |
     * #xA | #xD | [#x20 to #xD7FF] | [#xE000 to #xFFFD] | [#x10000 to #x10FFFF]
     * </p>
     *
     * <b>IMPORTANT:</b> The input object may be modified by the method. </p>
     *
     * @param sendMessageRequest
     *            Container for the necessary parameters to execute the
     *            SendMessage service method on AmazonSQS.
     *
     * @return The response from the SendMessage service method, as returned by
     *         AmazonSQS.
     *
     * @throws InvalidMessageContentsException
     * @throws UnsupportedOperationException
     *
     * @throws AmazonClientException
     *             If any internal errors are encountered inside the client
     *             while attempting to make the request or handle the response.
     *             For example if a network connection is not available.
     * @throws AmazonServiceException
     *             If an error response is returned by AmazonSQS indicating
     *             either a problem with the data in the request, or a server
     *             side issue.
     */
    public SendMessageResponse sendMessage(SendMessageRequest sendMessageRequest) {
        //TODO: Clone request since it's modified in this method and will cause issues if the client reuses request object.
        if (sendMessageRequest == null) {
            String errorMessage = "sendMessageRequest cannot be null.";
            LOG.error(errorMessage);
            throw new AmazonClientException(errorMessage);
        }

        SendMessageRequest.Builder sendMessageRequestBuilder = sendMessageRequest.toBuilder();
        sendMessageRequestBuilder.overrideConfiguration(
            AwsRequestOverrideConfiguration.builder()
                .putHeader(USER_AGENT_HEADER_NAME, USER_AGENT_HEADER)
                .build());
        sendMessageRequest = sendMessageRequestBuilder.build();

        if (!clientConfiguration.isPayloadSupportEnabled()) {
            return super.sendMessage(sendMessageRequest);
        }

        if (StringUtils.isNullOrEmpty(sendMessageRequest.messageBody())) {
            String errorMessage = "messageBody cannot be null or empty.";
            LOG.error(errorMessage);
            throw new AmazonClientException(errorMessage);
        }

        //Check message attributes for ExtendedClient related constraints
        checkMessageAttributes(sendMessageRequest.messageAttributes());

        if (clientConfiguration.isAlwaysThroughS3() || isLarge(sendMessageRequest)) {
            sendMessageRequest = storeMessageInS3(sendMessageRequest);
        }
        return super.sendMessage(sendMessageRequest);
    }

    /**
     * <p>
     * Retrieves one or more messages, with a maximum limit of 10 messages, from
     * the specified queue. Downloads the message payloads from Amazon S3 when
     * necessary. Long poll support is enabled by using the
     * <code>WaitTimeSeconds</code> parameter. For more information, see <a
     * href=
     * "http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html"
     * > Amazon SQS Long Poll </a> in the <i>Amazon SQS Developer Guide</i> .
     * </p>
     * <p>
     * Short poll is the default behavior where a weighted random set of
     * machines is sampled on a <code>ReceiveMessage</code> call. This means
     * only the messages on the sampled machines are returned. If the number of
     * messages in the queue is small (less than 1000), it is likely you will
     * get fewer messages than you requested per <code>ReceiveMessage</code>
     * call. If the number of messages in the queue is extremely small, you
     * might not receive any messages in a particular
     * <code>ReceiveMessage</code> response; in which case you should repeat the
     * request.
     * </p>
     * <p>
     * For each message returned, the response includes the following:
     * </p>
     *
     * <ul>
     * <li>
     * <p>
     * Message body
     * </p>
     * </li>
     * <li>
     * <p>
     * MD5 digest of the message body. For information about MD5, go to <a
     * href="http://www.faqs.org/rfcs/rfc1321.html">
     * http://www.faqs.org/rfcs/rfc1321.html </a> .
     * </p>
     * </li>
     * <li>
     * <p>
     * Message ID you received when you sent the message to the queue.
     * </p>
     * </li>
     * <li>
     * <p>
     * Receipt handle.
     * </p>
     * </li>
     * <li>
     * <p>
     * Message attributes.
     * </p>
     * </li>
     * <li>
     * <p>
     * MD5 digest of the message attributes.
     * </p>
     * </li>
     *
     * </ul>
     * <p>
     * The receipt handle is the identifier you must provide when deleting the
     * message. For more information, see <a href=
     * "http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/ImportantIdentifiers.html"
     * > Queue and Message Identifiers </a> in the <i>Amazon SQS Developer
     * Guide</i> .
     * </p>
     * <p>
     * You can provide the <code>VisibilityTimeout</code> parameter in your
     * request, which will be applied to the messages that Amazon SQS returns in
     * the response. If you do not include the parameter, the overall visibility
     * timeout for the queue is used for the returned messages. For more
     * information, see <a href=
     * "http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/AboutVT.html"
     * > Visibility Timeout </a> in the <i>Amazon SQS Developer Guide</i> .
     * </p>
     * <p>
     * <b>NOTE:</b> Going forward, new attributes might be added. If you are
     * writing code that calls this action, we recommend that you structure your
     * code so that it can handle new attributes gracefully.
     * </p>
     *
     * @param receiveMessageRequest
     *            Container for the necessary parameters to execute the
     *            ReceiveMessage service method on AmazonSQS.
     *
     * @return The response from the ReceiveMessage service method, as returned
     *         by AmazonSQS.
     *
     * @throws OverLimitException
     *
     * @throws AmazonClientException
     *             If any internal errors are encountered inside the client
     *             while attempting to make the request or handle the response.
     *             For example if a network connection is not available.
     * @throws AmazonServiceException
     *             If an error response is returned by AmazonSQS indicating
     *             either a problem with the data in the request, or a server
     *             side issue.
     */
    public ReceiveMessageResponse receiveMessage(ReceiveMessageRequest receiveMessageRequest) {
        //TODO: Clone request since it's modified in this method and will cause issues if the client reuses request object.
        if (receiveMessageRequest == null) {
            String errorMessage = "receiveMessageRequest cannot be null.";
            LOG.error(errorMessage);
            throw new AmazonClientException(errorMessage);
        }

        ReceiveMessageRequest.Builder receiveMessageRequestBuilder = receiveMessageRequest.toBuilder();
        receiveMessageRequestBuilder.overrideConfiguration(
            AwsRequestOverrideConfiguration.builder()
                .putHeader(USER_AGENT_HEADER_NAME, USER_AGENT_HEADER)
                .build());

        if (!clientConfiguration.isPayloadSupportEnabled()) {
            return super.receiveMessage(receiveMessageRequestBuilder.build());
        }
        //Remove before adding to avoid any duplicates
        List<String> messageAttributeNames = new ArrayList<>(receiveMessageRequest.messageAttributeNames());
        messageAttributeNames.removeAll(RESERVED_ATTRIBUTE_NAMES);
        messageAttributeNames.addAll(RESERVED_ATTRIBUTE_NAMES);
        receiveMessageRequestBuilder.attributeNamesWithStrings(messageAttributeNames);
        receiveMessageRequest = receiveMessageRequestBuilder.build();

        ReceiveMessageResponse receiveMessageResponse = super.receiveMessage(receiveMessageRequest);
        ReceiveMessageResponse.Builder receiveMessageResponseBuilder = receiveMessageResponse.toBuilder();

        List<Message> messages = receiveMessageResponse.messages();
        List<Message> modifiedMessages = new ArrayList<>(messages.size());
        for (Message message : messages) {
            Message.Builder messageBuilder = message.toBuilder();

            // for each received message check if they are stored in S3.
            Optional<String> largePayloadAttributeName = getReservedAttributeNameIfPresent(message.messageAttributes());
            if (largePayloadAttributeName.isPresent()) {
                String largeMessagePointer = message.body();

                messageBuilder.body(payloadStore.getOriginalPayload(largeMessagePointer));

                // remove the additional attribute before returning the message
                // to user.
                Map<String, MessageAttributeValue> messageAttributes = new HashMap<>(message.messageAttributes());
                messageAttributes.keySet().removeAll(RESERVED_ATTRIBUTE_NAMES);
                messageBuilder.messageAttributes(messageAttributes);

                // Embed s3 object pointer in the receipt handle.
                String modifiedReceiptHandle = embedS3PointerInReceiptHandle(
                        message.receiptHandle(),
                        largeMessagePointer);

                messageBuilder.receiptHandle(modifiedReceiptHandle);
            }
            modifiedMessages.add(messageBuilder.build());
        }

        receiveMessageResponseBuilder.messages(modifiedMessages);
        return receiveMessageResponseBuilder.build();
    }

    /**
     * <p>
     * Deletes the specified message from the specified queue and deletes the
     * message payload from Amazon S3 when necessary. You specify the message by
     * using the message's <code>receipt handle</code> and not the
     * <code>message ID</code> you received when you sent the message. Even if
     * the message is locked by another reader due to the visibility timeout
     * setting, it is still deleted from the queue. If you leave a message in
     * the queue for longer than the queue's configured retention period, Amazon
     * SQS automatically deletes it.
     * </p>
     * <p>
     * <b>NOTE:</b> The receipt handle is associated with a specific instance of
     * receiving the message. If you receive a message more than once, the
     * receipt handle you get each time you receive the message is different.
     * When you request DeleteMessage, if you don't provide the most recently
     * received receipt handle for the message, the request will still succeed,
     * but the message might not be deleted.
     * </p>
     * <p>
     * <b>IMPORTANT:</b> It is possible you will receive a message even after
     * you have deleted it. This might happen on rare occasions if one of the
     * servers storing a copy of the message is unavailable when you request to
     * delete the message. The copy remains on the server and might be returned
     * to you again on a subsequent receive request. You should create your
     * system to be idempotent so that receiving a particular message more than
     * once is not a problem.
     * </p>
     *
     * @param deleteMessageRequest
     *            Container for the necessary parameters to execute the
     *            DeleteMessage service method on AmazonSQS.
     *
     * @return The response from the DeleteMessage service method, as returned
     *         by AmazonSQS.
     *
     * @throws ReceiptHandleIsInvalidException
     * @throws InvalidIdFormatException
     *
     * @throws AmazonClientException
     *             If any internal errors are encountered inside the client
     *             while attempting to make the request or handle the response.
     *             For example if a network connection is not available.
     * @throws AmazonServiceException
     *             If an error response is returned by AmazonSQS indicating
     *             either a problem with the data in the request, or a server
     *             side issue.
     */
    public DeleteMessageResponse deleteMessage(DeleteMessageRequest deleteMessageRequest) {

        if (deleteMessageRequest == null) {
            String errorMessage = "deleteMessageRequest cannot be null.";
            LOG.error(errorMessage);
            throw new AmazonClientException(errorMessage);
        }

        DeleteMessageRequest.Builder deleteMessageRequestBuilder = deleteMessageRequest.toBuilder();
        deleteMessageRequestBuilder.overrideConfiguration(
            AwsRequestOverrideConfiguration.builder()
                .putHeader(USER_AGENT_HEADER_NAME, USER_AGENT_HEADER)
                .build());

        if (!clientConfiguration.isPayloadSupportEnabled()) {
            return super.deleteMessage(deleteMessageRequestBuilder.build());
        }

        String receiptHandle = deleteMessageRequest.receiptHandle();
        String origReceiptHandle = receiptHandle;

        // Update original receipt handle if needed
        if (isS3ReceiptHandle(receiptHandle)) {
            origReceiptHandle = getOrigReceiptHandle(receiptHandle);
            // Delete pay load from S3 if needed
            if (clientConfiguration.doesCleanupS3Payload()) {
                String messagePointer = getMessagePointerFromModifiedReceiptHandle(receiptHandle);
                payloadStore.deleteOriginalPayload(messagePointer);
            }
        }

        deleteMessageRequestBuilder.receiptHandle(origReceiptHandle);
        return super.deleteMessage(deleteMessageRequestBuilder.build());
    }

    /**
     * <p>
     * Changes the visibility timeout of a specified message in a queue to a new
     * value. The maximum allowed timeout value you can set the value to is 12
     * hours. This means you can't extend the timeout of a message in an
     * existing queue to more than a total visibility timeout of 12 hours. (For
     * more information visibility timeout, see <a href=
     * "http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/AboutVT.html"
     * > Visibility Timeout </a> in the <i>Amazon SQS Developer Guide</i> .)
     * </p>
     * <p>
     * For example, let's say you have a message and its default message
     * visibility timeout is 30 minutes. You could call
     * <code>ChangeMessageVisiblity</code> with a value of two hours and the
     * effective timeout would be two hours and 30 minutes. When that time comes
     * near you could again extend the time out by calling
     * ChangeMessageVisiblity, but this time the maximum allowed timeout would
     * be 9 hours and 30 minutes.
     * </p>
     * <p>
     * <b>NOTE:</b> There is a 120,000 limit for the number of inflight messages
     * per queue. Messages are inflight after they have been received from the
     * queue by a consuming component, but have not yet been deleted from the
     * queue. If you reach the 120,000 limit, you will receive an OverLimit
     * error message from Amazon SQS. To help avoid reaching the limit, you
     * should delete the messages from the queue after they have been processed.
     * You can also increase the number of queues you use to process the
     * messages.
     * </p>
     * <p>
     * <b>IMPORTANT:</b>If you attempt to set the VisibilityTimeout to an amount
     * more than the maximum time left, Amazon SQS returns an error. It will not
     * automatically recalculate and increase the timeout to the maximum time
     * remaining.
     * </p>
     * <p>
     * <b>IMPORTANT:</b>Unlike with a queue, when you change the visibility
     * timeout for a specific message, that timeout value is applied immediately
     * but is not saved in memory for that message. If you don't delete a
     * message after it is received, the visibility timeout for the message the
     * next time it is received reverts to the original timeout value, not the
     * value you set with the ChangeMessageVisibility action.
     * </p>
     *
     * @param changeMessageVisibilityRequest
     *            Container for the necessary parameters to execute the
     *            ChangeMessageVisibility service method on AmazonSQS.
     *
     *
     * @throws ReceiptHandleIsInvalidException
     * @throws MessageNotInflightException
     *
     * @throws AmazonClientException
     *             If any internal errors are encountered inside the client
     *             while attempting to make the request or handle the response.
     *             For example if a network connection is not available.
     * @throws AmazonServiceException
     *             If an error response is returned by AmazonSQS indicating
     *             either a problem with the data in the request, or a server
     *             side issue.
     */
    public ChangeMessageVisibilityResponse changeMessageVisibility(ChangeMessageVisibilityRequest changeMessageVisibilityRequest)
            throws AmazonServiceException, AmazonClientException {

        ChangeMessageVisibilityRequest.Builder changeMessageVisibilityRequestBuilder = changeMessageVisibilityRequest.toBuilder();
        if (isS3ReceiptHandle(changeMessageVisibilityRequest.receiptHandle())) {
            changeMessageVisibilityRequestBuilder.receiptHandle(
                    getOrigReceiptHandle(changeMessageVisibilityRequest.receiptHandle()));
        }
        return amazonSqsToBeExtended.changeMessageVisibility(changeMessageVisibilityRequestBuilder.build());
    }

    /**
     * <p>
     * Delivers up to ten messages to the specified queue. This is a batch
     * version of SendMessage. The result of the send action on each message is
     * reported individually in the response. Uploads message payloads to Amazon
     * S3 when necessary.
     * </p>
     * <p>
     * If the <code>DelaySeconds</code> parameter is not specified for an entry,
     * the default for the queue is used.
     * </p>
     * <p>
     * <b>IMPORTANT:</b>The following list shows the characters (in Unicode)
     * that are allowed in your message, according to the W3C XML specification.
     * For more information, go to http://www.faqs.org/rfcs/rfc1321.html. If you
     * send any characters that are not included in the list, your request will
     * be rejected. #x9 | #xA | #xD | [#x20 to #xD7FF] | [#xE000 to #xFFFD] |
     * [#x10000 to #x10FFFF]
     * </p>
     * <p>
     * <b>IMPORTANT:</b> Because the batch request can result in a combination
     * of successful and unsuccessful actions, you should check for batch errors
     * even when the call returns an HTTP status code of 200.
     * </p>
     * <b>IMPORTANT:</b> The input object may be modified by the method. </p>
     * <p>
     * <b>NOTE:</b>Some API actions take lists of parameters. These lists are
     * specified using the param.n notation. Values of n are integers starting
     * from 1. For example, a parameter list with two elements looks like this:
     * </p>
     * <p>
     * <code>&Attribute.1=this</code>
     * </p>
     * <p>
     * <code>&Attribute.2=that</code>
     * </p>
     *
     * @param sendMessageBatchRequest
     *            Container for the necessary parameters to execute the
     *            SendMessageBatch service method on AmazonSQS.
     *
     * @return The response from the SendMessageBatch service method, as
     *         returned by AmazonSQS.
     *
     * @throws BatchEntryIdsNotDistinctException
     * @throws TooManyEntriesInBatchRequestException
     * @throws BatchRequestTooLongException
     * @throws UnsupportedOperationException
     * @throws InvalidBatchEntryIdException
     * @throws EmptyBatchRequestException
     *
     * @throws AmazonClientException
     *             If any internal errors are encountered inside the client
     *             while attempting to make the request or handle the response.
     *             For example if a network connection is not available.
     * @throws AmazonServiceException
     *             If an error response is returned by AmazonSQS indicating
     *             either a problem with the data in the request, or a server
     *             side issue.
     */
    public SendMessageBatchResponse sendMessageBatch(SendMessageBatchRequest sendMessageBatchRequest) {

        if (sendMessageBatchRequest == null) {
            String errorMessage = "sendMessageBatchRequest cannot be null.";
            LOG.error(errorMessage);
            throw new AmazonClientException(errorMessage);
        }

        SendMessageBatchRequest.Builder sendMessageBatchRequestBuilder = sendMessageBatchRequest.toBuilder();
        sendMessageBatchRequestBuilder.overrideConfiguration(
            AwsRequestOverrideConfiguration.builder()
                .putHeader(USER_AGENT_HEADER_NAME, USER_AGENT_HEADER)
                .build());
        sendMessageBatchRequest = sendMessageBatchRequestBuilder.build();

        if (!clientConfiguration.isPayloadSupportEnabled()) {
            return super.sendMessageBatch(sendMessageBatchRequest);
        }

        List<SendMessageBatchRequestEntry> batchEntries = new ArrayList<>(sendMessageBatchRequest.entries().size());

        for (SendMessageBatchRequestEntry entry : sendMessageBatchRequest.entries()) {
            //Check message attributes for ExtendedClient related constraints
            checkMessageAttributes(entry.messageAttributes());

            if (clientConfiguration.isAlwaysThroughS3() || isLarge(entry)) {
                entry = storeMessageInS3(entry);
            }
            batchEntries.add(entry);
        }

        return super.sendMessageBatch(sendMessageBatchRequest);
    }

    /**
     * <p>
     * Deletes up to ten messages from the specified queue. This is a batch
     * version of DeleteMessage. The result of the delete action on each message
     * is reported individually in the response. Also deletes the message
     * payloads from Amazon S3 when necessary.
     * </p>
     * <p>
     * <b>IMPORTANT:</b> Because the batch request can result in a combination
     * of successful and unsuccessful actions, you should check for batch errors
     * even when the call returns an HTTP status code of 200.
     * </p>
     * <p>
     * <b>NOTE:</b>Some API actions take lists of parameters. These lists are
     * specified using the param.n notation. Values of n are integers starting
     * from 1. For example, a parameter list with two elements looks like this:
     * </p>
     * <p>
     * <code>&Attribute.1=this</code>
     * </p>
     * <p>
     * <code>&Attribute.2=that</code>
     * </p>
     *
     * @param deleteMessageBatchRequest
     *            Container for the necessary parameters to execute the
     *            DeleteMessageBatch service method on AmazonSQS.
     *
     * @return The response from the DeleteMessageBatch service method, as
     *         returned by AmazonSQS.
     *
     * @throws BatchEntryIdsNotDistinctException
     * @throws TooManyEntriesInBatchRequestException
     * @throws InvalidBatchEntryIdException
     * @throws EmptyBatchRequestException
     *
     * @throws AmazonClientException
     *             If any internal errors are encountered inside the client
     *             while attempting to make the request or handle the response.
     *             For example if a network connection is not available.
     * @throws AmazonServiceException
     *             If an error response is returned by AmazonSQS indicating
     *             either a problem with the data in the request, or a server
     *             side issue.
     */
    public DeleteMessageBatchResponse deleteMessageBatch(DeleteMessageBatchRequest deleteMessageBatchRequest) {

        if (deleteMessageBatchRequest == null) {
            String errorMessage = "deleteMessageBatchRequest cannot be null.";
            LOG.error(errorMessage);
            throw new AmazonClientException(errorMessage);
        }

        DeleteMessageBatchRequest.Builder deleteMessageBatchRequestBuilder = deleteMessageBatchRequest.toBuilder();
        deleteMessageBatchRequestBuilder.overrideConfiguration(
            AwsRequestOverrideConfiguration.builder()
                .putHeader(USER_AGENT_HEADER_NAME, USER_AGENT_HEADER)
                .build());

        if (!clientConfiguration.isPayloadSupportEnabled()) {
            return super.deleteMessageBatch(deleteMessageBatchRequest);
        }

        List<DeleteMessageBatchRequestEntry> entries = new ArrayList<>(deleteMessageBatchRequest.entries().size());
        for (DeleteMessageBatchRequestEntry entry : deleteMessageBatchRequest.entries()) {
            DeleteMessageBatchRequestEntry.Builder entryBuilder = entry.toBuilder();
            String receiptHandle = entry.receiptHandle();
            String origReceiptHandle = receiptHandle;

            // Update original receipt handle if needed
            if (isS3ReceiptHandle(receiptHandle)) {
                origReceiptHandle = getOrigReceiptHandle(receiptHandle);
                // Delete s3 payload if needed
                if (clientConfiguration.doesCleanupS3Payload()) {
                    String messagePointer = getMessagePointerFromModifiedReceiptHandle(receiptHandle);
                    payloadStore.deleteOriginalPayload(messagePointer);
                }
            }

            entryBuilder.receiptHandle(origReceiptHandle);
            entries.add(entryBuilder.build());
        }
        // MIKE FIX
        //deleteMessageBatchRequestBuilder.entries(entries);
        return super.deleteMessageBatch(deleteMessageBatchRequestBuilder.build());
    }

    /**
     * <p>
     * Changes the visibility timeout of multiple messages. This is a batch
     * version of ChangeMessageVisibility. The result of the action on each
     * message is reported individually in the response. You can send up to 10
     * ChangeMessageVisibility requests with each
     * <code>ChangeMessageVisibilityBatch</code> action.
     * </p>
     * <p>
     * <b>IMPORTANT:</b>Because the batch request can result in a combination of
     * successful and unsuccessful actions, you should check for batch errors
     * even when the call returns an HTTP status code of 200.
     * </p>
     * <p>
     * <b>NOTE:</b>Some API actions take lists of parameters. These lists are
     * specified using the param.n notation. Values of n are integers starting
     * from 1. For example, a parameter list with two elements looks like this:
     * </p>
     * <p>
     * <code>&Attribute.1=this</code>
     * </p>
     * <p>
     * <code>&Attribute.2=that</code>
     * </p>
     *
     * @param changeMessageVisibilityBatchRequest
     *            Container for the necessary parameters to execute the
     *            ChangeMessageVisibilityBatch service method on AmazonSQS.
     *
     * @return The response from the ChangeMessageVisibilityBatch service
     *         method, as returned by AmazonSQS.
     *
     * @throws BatchEntryIdsNotDistinctException
     * @throws TooManyEntriesInBatchRequestException
     * @throws InvalidBatchEntryIdException
     * @throws EmptyBatchRequestException
     *
     * @throws AmazonClientException
     *             If any internal errors are encountered inside the client
     *             while attempting to make the request or handle the response.
     *             For example if a network connection is not available.
     * @throws AmazonServiceException
     *             If an error response is returned by AmazonSQS indicating
     *             either a problem with the data in the request, or a server
     *             side issue.
     */
    public ChangeMessageVisibilityBatchResponse changeMessageVisibilityBatch(
            ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest) throws AmazonServiceException,
            AmazonClientException {

        List<ChangeMessageVisibilityBatchRequestEntry> entries = new ArrayList<>(changeMessageVisibilityBatchRequest.entries().size());
        for (ChangeMessageVisibilityBatchRequestEntry entry : changeMessageVisibilityBatchRequest.entries()) {
            ChangeMessageVisibilityBatchRequestEntry.Builder entryBuilder = entry.toBuilder();
            if (isS3ReceiptHandle(entry.receiptHandle())) {
                entryBuilder.receiptHandle(getOrigReceiptHandle(entry.receiptHandle()));
            }
            entries.add(entryBuilder.build());
        }

        return amazonSqsToBeExtended.changeMessageVisibilityBatch(
            changeMessageVisibilityBatchRequest.toBuilder().entries(entries).build());
    }

    /**
     * <p>
     * Deletes the messages in a queue specified by the <b>queue URL</b> .
     * </p>
     * <p>
     * <b>IMPORTANT:</b>When you use the PurgeQueue API, the deleted messages in
     * the queue cannot be retrieved.
     * </p>
     * <p>
     * <b>IMPORTANT:</b> This does not delete the message payloads from Amazon S3.
     * </p>
     * <p>
     * When you purge a queue, the message deletion process takes up to 60
     * seconds. All messages sent to the queue before calling
     * <code>PurgeQueue</code> will be deleted; messages sent to the queue while
     * it is being purged may be deleted. While the queue is being purged,
     * messages sent to the queue before <code>PurgeQueue</code> was called may
     * be received, but will be deleted within the next minute.
     * </p>
     *
     * @param purgeQueueRequest
     *            Container for the necessary parameters to execute the
     *            PurgeQueue service method on AmazonSQS.
     * @return The response from the PurgeQueue service method, as returned
     *         by AmazonSQS.
     *
     * @throws AmazonClientException
     *             If any internal errors are encountered inside the client
     *             while attempting to make the request or handle the response.
     *             For example if a network connection is not available.
     * @throws AmazonServiceException
     *             If an error response is returned by AmazonSQS indicating
     *             either a problem with the data in the request, or a server
     *             side issue.
     */
    public PurgeQueueResponse purgeQueue(PurgeQueueRequest purgeQueueRequest)
            throws AmazonServiceException, AmazonClientException {
        LOG.warn("Calling purgeQueue deletes SQS messages without deleting their payload from S3.");

        if (purgeQueueRequest == null) {
            String errorMessage = "purgeQueueRequest cannot be null.";
            LOG.error(errorMessage);
            throw new AmazonClientException(errorMessage);
        }

        PurgeQueueRequest.Builder purgeQueueRequestBuilder = purgeQueueRequest.toBuilder();
        purgeQueueRequestBuilder.overrideConfiguration(
            AwsRequestOverrideConfiguration.builder()
                .putHeader(USER_AGENT_HEADER_NAME, USER_AGENT_HEADER)
                .build());

        return super.purgeQueue(purgeQueueRequestBuilder.build());
    }

    private void checkMessageAttributes(Map<String, MessageAttributeValue> messageAttributes) {
        int msgAttributesSize = getMsgAttributesSize(messageAttributes);
        if (msgAttributesSize > clientConfiguration.getPayloadSizeThreshold()) {
            String errorMessage = "Total size of Message attributes is " + msgAttributesSize
                    + " bytes which is larger than the threshold of " + clientConfiguration.getPayloadSizeThreshold()
                    + " Bytes. Consider including the payload in the message body instead of message attributes.";
            LOG.error(errorMessage);
            throw new AmazonClientException(errorMessage);
        }

        int messageAttributesNum = messageAttributes.size();
        if (messageAttributesNum > SQSExtendedClientConstants.MAX_ALLOWED_ATTRIBUTES) {
            String errorMessage = "Number of message attributes [" + messageAttributesNum
                    + "] exceeds the maximum allowed for large-payload messages ["
                    + SQSExtendedClientConstants.MAX_ALLOWED_ATTRIBUTES + "].";
            LOG.error(errorMessage);
            throw new AmazonClientException(errorMessage);
        }
        Optional<String> largePayloadAttributeName = getReservedAttributeNameIfPresent(messageAttributes);

        if (largePayloadAttributeName.isPresent()) {
            String errorMessage = "Message attribute name " + largePayloadAttributeName.get()
                    + " is reserved for use by SQS extended client.";
            LOG.error(errorMessage);
            throw new AmazonClientException(errorMessage);
        }
    }

    /**
     * TODO: Wrap the message pointer as-is to the receiptHandle so that it can be generic
     * and does not use any LargeMessageStore implementation specific details.
     */
    private String embedS3PointerInReceiptHandle(String receiptHandle, String pointer) {
        PayloadS3Pointer s3Pointer = PayloadS3Pointer.fromJson(pointer);
        String s3MsgBucketName = s3Pointer.getS3BucketName();
        String s3MsgKey = s3Pointer.getS3Key();

        String modifiedReceiptHandle = SQSExtendedClientConstants.S3_BUCKET_NAME_MARKER + s3MsgBucketName
                + SQSExtendedClientConstants.S3_BUCKET_NAME_MARKER + SQSExtendedClientConstants.S3_KEY_MARKER
                + s3MsgKey + SQSExtendedClientConstants.S3_KEY_MARKER + receiptHandle;
        return modifiedReceiptHandle;
    }

    private String getOrigReceiptHandle(String receiptHandle) {
        int secondOccurence = receiptHandle.indexOf(SQSExtendedClientConstants.S3_KEY_MARKER,
                receiptHandle.indexOf(SQSExtendedClientConstants.S3_KEY_MARKER) + 1);
        return receiptHandle.substring(secondOccurence + SQSExtendedClientConstants.S3_KEY_MARKER.length());
    }

    private String getFromReceiptHandleByMarker(String receiptHandle, String marker) {
        int firstOccurence = receiptHandle.indexOf(marker);
        int secondOccurence = receiptHandle.indexOf(marker, firstOccurence + 1);
        return receiptHandle.substring(firstOccurence + marker.length(), secondOccurence);
    }

    private boolean isS3ReceiptHandle(String receiptHandle) {
        return receiptHandle.contains(SQSExtendedClientConstants.S3_BUCKET_NAME_MARKER)
                && receiptHandle.contains(SQSExtendedClientConstants.S3_KEY_MARKER);
    }

    private String getMessagePointerFromModifiedReceiptHandle(String receiptHandle) {
        String s3MsgBucketName = getFromReceiptHandleByMarker(receiptHandle, SQSExtendedClientConstants.S3_BUCKET_NAME_MARKER);
        String s3MsgKey = getFromReceiptHandleByMarker(receiptHandle, SQSExtendedClientConstants.S3_KEY_MARKER);

        PayloadS3Pointer payloadS3Pointer = new PayloadS3Pointer(s3MsgBucketName, s3MsgKey);
        return payloadS3Pointer.toJson();
    }

    private boolean isLarge(SendMessageRequest sendMessageRequest) {
        int msgAttributesSize = getMsgAttributesSize(sendMessageRequest.messageAttributes());
        long msgBodySize = Util.getStringSizeInBytes(sendMessageRequest.messageBody());
        long totalMsgSize = msgAttributesSize + msgBodySize;
        return (totalMsgSize > clientConfiguration.getPayloadSizeThreshold());
    }

    private boolean isLarge(SendMessageBatchRequestEntry batchEntry) {
        int msgAttributesSize = getMsgAttributesSize(batchEntry.messageAttributes());
        long msgBodySize = Util.getStringSizeInBytes(batchEntry.messageBody());
        long totalMsgSize = msgAttributesSize + msgBodySize;
        return (totalMsgSize > clientConfiguration.getPayloadSizeThreshold());
    }

    private Optional<String> getReservedAttributeNameIfPresent(Map<String, MessageAttributeValue> msgAttributes) {
        String reservedAttributeName = null;
        if (msgAttributes.containsKey(SQSExtendedClientConstants.RESERVED_ATTRIBUTE_NAME)) {
            reservedAttributeName = SQSExtendedClientConstants.RESERVED_ATTRIBUTE_NAME;
        } else if (msgAttributes.containsKey(LEGACY_RESERVED_ATTRIBUTE_NAME)) {
            reservedAttributeName = LEGACY_RESERVED_ATTRIBUTE_NAME;
        }
        return Optional.ofNullable(reservedAttributeName);
    }

    private int getMsgAttributesSize(Map<String, MessageAttributeValue> msgAttributes) {
        int totalMsgAttributesSize = 0;
        for (Map.Entry<String, MessageAttributeValue> entry : msgAttributes.entrySet()) {
            totalMsgAttributesSize += Util.getStringSizeInBytes(entry.getKey());

            MessageAttributeValue entryVal = entry.getValue();
            if (entryVal.dataType() != null) {
                totalMsgAttributesSize += Util.getStringSizeInBytes(entryVal.dataType());
            }

            String stringVal = entryVal.stringValue();
            if (stringVal != null) {
                totalMsgAttributesSize += Util.getStringSizeInBytes(entryVal.stringValue());
            }

            SdkBytes binaryVal = entryVal.binaryValue();
            if (binaryVal != null) {
                totalMsgAttributesSize += binaryVal.asByteArray().length;
            }
        }
        return totalMsgAttributesSize;
    }

    private SendMessageBatchRequestEntry storeMessageInS3(SendMessageBatchRequestEntry batchEntry) {

        // Read the content of the message from message body
        String messageContentStr = batchEntry.messageBody();

        Long messageContentSize = Util.getStringSizeInBytes(messageContentStr);

        SendMessageBatchRequestEntry.Builder batchEntryBuilder = batchEntry.toBuilder();

        batchEntryBuilder.messageAttributes(
            updateMessageAttributePayloadSize(batchEntry.messageAttributes(), messageContentSize));

        // Store the message content in S3.
        String largeMessagePointer = payloadStore.storeOriginalPayload(messageContentStr,
                messageContentSize);
        batchEntryBuilder.messageBody(largeMessagePointer);

        return batchEntryBuilder.build();
    }

    private SendMessageRequest storeMessageInS3(SendMessageRequest sendMessageRequest) {

        // Read the content of the message from message body
        String messageContentStr = sendMessageRequest.messageBody();

        Long messageContentSize = Util.getStringSizeInBytes(messageContentStr);

        SendMessageRequest.Builder sendMessageRequestBuilder = sendMessageRequest.toBuilder();

        sendMessageRequestBuilder.messageAttributes(
            updateMessageAttributePayloadSize(sendMessageRequest.messageAttributes(), messageContentSize));

        // Store the message content in S3.
        String largeMessagePointer = payloadStore.storeOriginalPayload(messageContentStr,
                messageContentSize);
        sendMessageRequestBuilder.messageBody(largeMessagePointer);

        return sendMessageRequestBuilder.build();
    }

    private Map<String, MessageAttributeValue> updateMessageAttributePayloadSize(
        Map<String, MessageAttributeValue> messageAttributes, Long messageContentSize) {
        Map<String, MessageAttributeValue> updatedMessageAttributes = new HashMap<>(messageAttributes);

        // Add a new message attribute as a flag
        MessageAttributeValue.Builder messageAttributeValueBuilder = MessageAttributeValue.builder();
        messageAttributeValueBuilder.dataType("Number");
        messageAttributeValueBuilder.stringValue(messageContentSize.toString());
        MessageAttributeValue messageAttributeValue = messageAttributeValueBuilder.build();

        if (!clientConfiguration.usesLegacyReservedAttributeName()) {
            updatedMessageAttributes.put(SQSExtendedClientConstants.RESERVED_ATTRIBUTE_NAME, messageAttributeValue);
        } else {
            updatedMessageAttributes.put(LEGACY_RESERVED_ATTRIBUTE_NAME, messageAttributeValue);
        }
        return updatedMessageAttributes;
    }
}
