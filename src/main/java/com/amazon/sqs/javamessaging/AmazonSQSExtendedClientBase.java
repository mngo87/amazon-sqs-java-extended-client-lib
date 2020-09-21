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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

abstract class AmazonSQSExtendedClientBase implements SqsClient {
    SqsClient amazonSqsToBeExtended;

    public AmazonSQSExtendedClientBase(SqsClient sqsClient) {
        amazonSqsToBeExtended = sqsClient;
    }

    /**
     * <p>
     * Delivers a message to the specified queue. With Amazon SQS, you now
     * have the ability to send large payload messages that are up to 256KB
     * (262,144 bytes) in size. To send large payloads, you must use an AWS
     * SDK that supports SigV4 signing. To verify whether SigV4 is supported
     * for an AWS SDK, check the SDK release notes.
     * </p>
     * <p>
     * <b>IMPORTANT:</b> The following list shows the characters (in
     * Unicode) allowed in your message, according to the W3C XML
     * specification. For more information, go to
     * http://www.w3.org/TR/REC-xml/#charsets If you send any characters not
     * included in the list, your request will be rejected. #x9 | #xA | #xD |
     * [#x20 to #xD7FF] | [#xE000 to #xFFFD] | [#x10000 to #x10FFFF]
     * </p>
     *
     * @param sendMessageRequest Container for the necessary parameters to
     *           execute the SendMessage service method on AmazonSQS.
     *
     * @return The response from the SendMessage service method, as returned
     *         by AmazonSQS.
     *
     * @throws InvalidMessageContentsException
     * @throws UnsupportedOperationException
     *
     * @throws AmazonClientException
     *             If any internal errors are encountered inside the client while
     *             attempting to make the request or handle the response.  For example
     *             if a network connection is not available.
     * @throws AmazonServiceException
     *             If an error response is returned by AmazonSQS indicating
     *             either a problem with the data in the request, or a server side issue.
     */
    public SendMessageResponse sendMessage(SendMessageRequest sendMessageRequest) {
        return amazonSqsToBeExtended.sendMessage(sendMessageRequest);
    }

    /**
     * <p>
     * Retrieves one or more messages, with a maximum limit of 10 messages,
     * from the specified queue. Long poll support is enabled by using the
     * <code>WaitTimeSeconds</code> parameter. For more information, see
     * <a href="http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html"> Amazon SQS Long Poll </a>
     * in the <i>Amazon SQS Developer Guide</i> .
     * </p>
     * <p>
     * Short poll is the default behavior where a weighted random set of
     * machines is sampled on a <code>ReceiveMessage</code> call. This means
     * only the messages on the sampled machines are returned. If the number
     * of messages in the queue is small (less than 1000), it is likely you
     * will get fewer messages than you requested per
     * <code>ReceiveMessage</code> call. If the number of messages in the
     * queue is extremely small, you might not receive any messages in a
     * particular <code>ReceiveMessage</code> response; in which case you
     * should repeat the request.
     * </p>
     * <p>
     * For each message returned, the response includes the following:
     * </p>
     *
     * <ul>
     * <li> <p>
     * Message body
     * </p>
     * </li>
     * <li> <p>
     * MD5 digest of the message body. For information about MD5, go to
     * <a href="http://www.faqs.org/rfcs/rfc1321.html"> http://www.faqs.org/rfcs/rfc1321.html </a>
     * .
     * </p>
     * </li>
     * <li> <p>
     * Message ID you received when you sent the message to the queue.
     * </p>
     * </li>
     * <li> <p>
     * Receipt handle.
     * </p>
     * </li>
     * <li> <p>
     * Message attributes.
     * </p>
     * </li>
     * <li> <p>
     * MD5 digest of the message attributes.
     * </p>
     * </li>
     *
     * </ul>
     * <p>
     * The receipt handle is the identifier you must provide when deleting
     * the message. For more information, see
     * <a href="http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/ImportantIdentifiers.html"> Queue and Message Identifiers </a>
     * in the <i>Amazon SQS Developer Guide</i> .
     * </p>
     * <p>
     * You can provide the <code>VisibilityTimeout</code> parameter in your
     * request, which will be applied to the messages that Amazon SQS returns
     * in the response. If you do not include the parameter, the overall
     * visibility timeout for the queue is used for the returned messages.
     * For more information, see
     * <a href="http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/AboutVT.html"> Visibility Timeout </a>
     * in the <i>Amazon SQS Developer Guide</i> .
     * </p>
     * <p>
     * <b>NOTE:</b> Going forward, new attributes might be added. If you are
     * writing code that calls this action, we recommend that you structure
     * your code so that it can handle new attributes gracefully.
     * </p>
     *
     * @param receiveMessageRequest Container for the necessary parameters to
     *           execute the ReceiveMessage service method on AmazonSQS.
     *
     * @return The response from the ReceiveMessage service method, as
     *         returned by AmazonSQS.
     *
     * @throws OverLimitException
     *
     * @throws AmazonClientException
     *             If any internal errors are encountered inside the client while
     *             attempting to make the request or handle the response.  For example
     *             if a network connection is not available.
     * @throws AmazonServiceException
     *             If an error response is returned by AmazonSQS indicating
     *             either a problem with the data in the request, or a server side issue.
     */
    public ReceiveMessageResponse receiveMessage(ReceiveMessageRequest receiveMessageRequest) {
        return amazonSqsToBeExtended.receiveMessage(receiveMessageRequest);
    }

    /**
     * <p>
     * Deletes the specified message from the specified queue. You specify
     * the message by using the message's <code>receipt handle</code> and not
     * the <code>message ID</code> you received when you sent the message.
     * Even if the message is locked by another reader due to the visibility
     * timeout setting, it is still deleted from the queue. If you leave a
     * message in the queue for longer than the queue's configured retention
     * period, Amazon SQS automatically deletes it.
     * </p>
     * <p>
     * <b>NOTE:</b> The receipt handle is associated with a specific
     * instance of receiving the message. If you receive a message more than
     * once, the receipt handle you get each time you receive the message is
     * different. When you request DeleteMessage, if you don't provide the
     * most recently received receipt handle for the message, the request
     * will still succeed, but the message might not be deleted.
     * </p>
     * <p>
     * <b>IMPORTANT:</b> It is possible you will receive a message even
     * after you have deleted it. This might happen on rare occasions if one
     * of the servers storing a copy of the message is unavailable when you
     * request to delete the message. The copy remains on the server and
     * might be returned to you again on a subsequent receive request. You
     * should create your system to be idempotent so that receiving a
     * particular message more than once is not a problem.
     * </p>
     *
     * @param deleteMessageRequest Container for the necessary parameters to
     *           execute the DeleteMessage service method on AmazonSQS.
     *
     * @return The response from the DeleteMessage service method, as returned
     *         by AmazonSQS.
     *
     * @throws ReceiptHandleIsInvalidException
     * @throws InvalidIdFormatException
     *
     * @throws AmazonClientException
     *             If any internal errors are encountered inside the client while
     *             attempting to make the request or handle the response.  For example
     *             if a network connection is not available.
     * @throws AmazonServiceException
     *             If an error response is returned by AmazonSQS indicating
     *             either a problem with the data in the request, or a server side issue.
     */
    public DeleteMessageResponse deleteMessage(DeleteMessageRequest deleteMessageRequest) {
        return amazonSqsToBeExtended.deleteMessage(deleteMessageRequest);
    }

    /**
     * <p>
     * Sets the value of one or more queue attributes. When you change a queue's
     * attributes, the change can take up to 60 seconds for most of the
     * attributes to propagate throughout the SQS system. Changes made to the
     * <code>MessageRetentionPeriod</code> attribute can take up to 15 minutes.
     * </p>
     * <p>
     * <b>NOTE:</b>Going forward, new attributes might be added. If you are
     * writing code that calls this action, we recommend that you structure your
     * code so that it can handle new attributes gracefully.
     * </p>
     *
     * @param setQueueAttributesRequest
     *            Container for the necessary parameters to execute the
     *            SetQueueAttributes service method on AmazonSQS.
     *
     *
     * @throws InvalidAttributeNameException
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
    public SetQueueAttributesResponse setQueueAttributes(SetQueueAttributesRequest setQueueAttributesRequest)
            throws AmazonServiceException, AmazonClientException {

        return amazonSqsToBeExtended.setQueueAttributes(setQueueAttributesRequest);
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

        return amazonSqsToBeExtended.changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
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

        return amazonSqsToBeExtended.changeMessageVisibility(changeMessageVisibilityRequest);
    }

    /**
     * <p>
     * Returns the URL of an existing queue. This action provides a simple way
     * to retrieve the URL of an Amazon SQS queue.
     * </p>
     * <p>
     * To access a queue that belongs to another AWS account, use the
     * <code>QueueOwnerAWSAccountId</code> parameter to specify the account ID
     * of the queue's owner. The queue's owner must grant you permission to
     * access the queue. For more information about shared queue access, see
     * AddPermission or go to <a href=
     * "http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/acp-overview.html"
     * > Shared Queues </a> in the <i>Amazon SQS Developer Guide</i> .
     * </p>
     *
     * @param getQueueUrlRequest
     *            Container for the necessary parameters to execute the
     *            GetQueueUrl service method on AmazonSQS.
     *
     * @return The response from the GetQueueUrl service method, as returned by
     *         AmazonSQS.
     *
     * @throws QueueDoesNotExistException
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
    public GetQueueUrlResponse getQueueUrl(GetQueueUrlRequest getQueueUrlRequest) throws AmazonServiceException,
            AmazonClientException {

        return amazonSqsToBeExtended.getQueueUrl(getQueueUrlRequest);
    }

    /**
     * <p>
     * Revokes any permissions in the queue policy that matches the specified
     * <code>Label</code> parameter. Only the owner of the queue can remove
     * permissions.
     * </p>
     *
     * @param removePermissionRequest
     *            Container for the necessary parameters to execute the
     *            RemovePermission service method on AmazonSQS.
     *
     *
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
    public RemovePermissionResponse removePermission(RemovePermissionRequest removePermissionRequest)
            throws AmazonServiceException, AmazonClientException {

        return amazonSqsToBeExtended.removePermission(removePermissionRequest);
    }

    /**
     * <p>
     * Gets attributes for the specified queue. The following attributes are
     * supported:
     * <ul>
     * <li> <code>All</code> - returns all values.</li>
     * <li> <code>ApproximateNumberOfMessages</code> - returns the approximate
     * number of visible messages in a queue. For more information, see <a href=
     * "http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/ApproximateNumber.html"
     * > Resources Required to Process Messages </a> in the <i>Amazon SQS
     * Developer Guide</i> .</li>
     * <li> <code>ApproximateNumberOfMessagesNotVisible</code> - returns the
     * approximate number of messages that are not timed-out and not deleted.
     * For more information, see <a href=
     * "http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/ApproximateNumber.html"
     * > Resources Required to Process Messages </a> in the <i>Amazon SQS
     * Developer Guide</i> .</li>
     * <li> <code>VisibilityTimeout</code> - returns the visibility timeout for
     * the queue. For more information about visibility timeout, see <a href=
     * "http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/AboutVT.html"
     * > Visibility Timeout </a> in the <i>Amazon SQS Developer Guide</i> .</li>
     * <li> <code>CreatedTimestamp</code> - returns the time when the queue was
     * created (epoch time in seconds).</li>
     * <li> <code>LastModifiedTimestamp</code> - returns the time when the queue
     * was last changed (epoch time in seconds).</li>
     * <li> <code>Policy</code> - returns the queue's policy.</li>
     * <li> <code>MaximumMessageSize</code> - returns the limit of how many bytes
     * a message can contain before Amazon SQS rejects it.</li>
     * <li> <code>MessageRetentionPeriod</code> - returns the number of seconds
     * Amazon SQS retains a message.</li>
     * <li> <code>QueueArn</code> - returns the queue's Amazon resource name
     * (ARN).</li>
     * <li> <code>ApproximateNumberOfMessagesDelayed</code> - returns the
     * approximate number of messages that are pending to be added to the queue.
     * </li>
     * <li> <code>DelaySeconds</code> - returns the default delay on the queue in
     * seconds.</li>
     * <li> <code>ReceiveMessageWaitTimeSeconds</code> - returns the time for
     * which a ReceiveMessage call will wait for a message to arrive.</li>
     * <li> <code>RedrivePolicy</code> - returns the parameters for dead letter
     * queue functionality of the source queue. For more information about
     * RedrivePolicy and dead letter queues, see <a href=
     * "http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/SQSDeadLetterQueue.html"
     * > Using Amazon SQS Dead Letter Queues </a> in the <i>Amazon SQS Developer
     * Guide</i> .</li>
     *
     * </ul>
     *
     * </p>
     * <p>
     * <b>NOTE:</b>Going forward, new attributes might be added. If you are
     * writing code that calls this action, we recommend that you structure your
     * code so that it can handle new attributes gracefully.
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
     * @param getQueueAttributesRequest
     *            Container for the necessary parameters to execute the
     *            GetQueueAttributes service method on AmazonSQS.
     *
     * @return The response from the GetQueueAttributes service method, as
     *         returned by AmazonSQS.
     *
     * @throws InvalidAttributeNameException
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
    public GetQueueAttributesResponse getQueueAttributes(GetQueueAttributesRequest getQueueAttributesRequest)
            throws AmazonServiceException, AmazonClientException {

        return amazonSqsToBeExtended.getQueueAttributes(getQueueAttributesRequest);
    }

    /**
     * <p>
     * Delivers up to ten messages to the specified queue. This is a batch
     * version of SendMessage. The result of the send action on each message is
     * reported individually in the response. The maximum allowed individual
     * message size is 256 KB (262,144 bytes).
     * </p>
     * <p>
     * The maximum total payload size (i.e., the sum of all a batch's individual
     * message lengths) is also 256 KB (262,144 bytes).
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
    public SendMessageBatchResponse sendMessageBatch(SendMessageBatchRequest sendMessageBatchRequest)
            throws AmazonServiceException, AmazonClientException {

        return amazonSqsToBeExtended.sendMessageBatch(sendMessageBatchRequest);
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
     *
     *
     * @throws PurgeQueueInProgressException
     * @throws QueueDoesNotExistException
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

        return amazonSqsToBeExtended.purgeQueue(purgeQueueRequest);

    }

    /**
     * <p>
     * Returns a list of your queues that have the RedrivePolicy queue attribute
     * configured with a dead letter queue.
     * </p>
     * <p>
     * For more information about using dead letter queues, see <a href=
     * "http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/SQSDeadLetterQueue.html"
     * > Using Amazon SQS Dead Letter Queues </a> .
     * </p>
     *
     * @param listDeadLetterSourceQueuesRequest
     *            Container for the necessary parameters to execute the
     *            ListDeadLetterSourceQueues service method on AmazonSQS.
     *
     * @return The response from the ListDeadLetterSourceQueues service method,
     *         as returned by AmazonSQS.
     *
     * @throws QueueDoesNotExistException
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
    public ListDeadLetterSourceQueuesResponse listDeadLetterSourceQueues(
            ListDeadLetterSourceQueuesRequest listDeadLetterSourceQueuesRequest) throws AmazonServiceException,
            AmazonClientException {

        return amazonSqsToBeExtended.listDeadLetterSourceQueues(listDeadLetterSourceQueuesRequest);
    }

    /**
     * <p>
     * Deletes the queue specified by the <b>queue URL</b> , regardless of
     * whether the queue is empty. If the specified queue does not exist, Amazon
     * SQS returns a successful response.
     * </p>
     * <p>
     * <b>IMPORTANT:</b> Use DeleteQueue with care; once you delete your queue,
     * any messages in the queue are no longer available.
     * </p>
     * <p>
     * When you delete a queue, the deletion process takes up to 60 seconds.
     * Requests you send involving that queue during the 60 seconds might
     * succeed. For example, a SendMessage request might succeed, but after the
     * 60 seconds, the queue and that message you sent no longer exist. Also,
     * when you delete a queue, you must wait at least 60 seconds before
     * creating a queue with the same name.
     * </p>
     * <p>
     * We reserve the right to delete queues that have had no activity for more
     * than 30 days. For more information, see <a href=
     * "http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/SQSConcepts.html"
     * > How Amazon SQS Queues Work </a> in the <i>Amazon SQS Developer
     * Guide</i> .
     * </p>
     *
     * @param deleteQueueRequest
     *            Container for the necessary parameters to execute the
     *            DeleteQueue service method on AmazonSQS.
     *
     *
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
    public DeleteQueueResponse deleteQueue(DeleteQueueRequest deleteQueueRequest)
            throws AmazonServiceException, AmazonClientException {

        return amazonSqsToBeExtended.deleteQueue(deleteQueueRequest);
    }

    /**
     * <p>
     * Returns a list of your queues. The maximum number of queues that can be
     * returned is 1000. If you specify a value for the optional
     * <code>QueueNamePrefix</code> parameter, only queues with a name beginning
     * with the specified value are returned.
     * </p>
     *
     * @param listQueuesRequest
     *            Container for the necessary parameters to execute the
     *            ListQueues service method on AmazonSQS.
     *
     * @return The response from the ListQueues service method, as returned by
     *         AmazonSQS.
     *
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
    public ListQueuesResponse listQueues(ListQueuesRequest listQueuesRequest) throws AmazonServiceException,
            AmazonClientException {

        return amazonSqsToBeExtended.listQueues(listQueuesRequest);
    }

    /**
     * <p>
     * Deletes up to ten messages from the specified queue. This is a batch
     * version of DeleteMessage. The result of the delete action on each message
     * is reported individually in the response.
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
    public DeleteMessageBatchResponse deleteMessageBatch(DeleteMessageBatchRequest deleteMessageBatchRequest)
            throws AmazonServiceException, AmazonClientException {

        return amazonSqsToBeExtended.deleteMessageBatch(deleteMessageBatchRequest);
    }

    /**
     * <p>
     * Creates a new queue, or returns the URL of an existing one. When you
     * request <code>CreateQueue</code> , you provide a name for the queue. To
     * successfully create a new queue, you must provide a name that is unique
     * within the scope of your own queues.
     * </p>
     * <p>
     * <b>NOTE:</b> If you delete a queue, you must wait at least 60 seconds
     * before creating a queue with the same name.
     * </p>
     * <p>
     * You may pass one or more attributes in the request. If you do not provide
     * a value for any attribute, the queue will have the default value for that
     * attribute. Permitted attributes are the same that can be set using
     * SetQueueAttributes.
     * </p>
     * <p>
     * <b>NOTE:</b> Use GetQueueUrl to get a queue's URL. GetQueueUrl requires
     * only the QueueName parameter.
     * </p>
     * <p>
     * If you provide the name of an existing queue, along with the exact names
     * and values of all the queue's attributes, <code>CreateQueue</code>
     * returns the queue URL for the existing queue. If the queue name,
     * attribute names, or attribute values do not match an existing queue,
     * <code>CreateQueue</code> returns an error.
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
     * @param createQueueRequest
     *            Container for the necessary parameters to execute the
     *            CreateQueue service method on AmazonSQS.
     *
     * @return The response from the CreateQueue service method, as returned by
     *         AmazonSQS.
     *
     * @throws QueueNameExistsException
     * @throws QueueDeletedRecentlyException
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
    public CreateQueueResponse createQueue(CreateQueueRequest createQueueRequest) throws AmazonServiceException,
            AmazonClientException {

        return amazonSqsToBeExtended.createQueue(createQueueRequest);
    }

    /**
     * <p>
     * Adds a permission to a queue for a specific <a
     * href="http://docs.aws.amazon.com/general/latest/gr/glos-chap.html#P">
     * principal </a> . This allows for sharing access to the queue.
     * </p>
     * <p>
     * When you create a queue, you have full control access rights for the
     * queue. Only you (as owner of the queue) can grant or deny permissions to
     * the queue. For more information about these permissions, see <a href=
     * "http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/acp-overview.html"
     * > Shared Queues </a> in the <i>Amazon SQS Developer Guide</i> .
     * </p>
     * <p>
     * <b>NOTE:</b> AddPermission writes an Amazon SQS-generated policy. If you
     * want to write your own policy, use SetQueueAttributes to upload your
     * policy. For more information about writing your own policy, see Using The
     * Access Policy Language in the Amazon SQS Developer Guide.
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
     * @param addPermissionRequest
     *            Container for the necessary parameters to execute the
     *            AddPermission service method on AmazonSQS.
     *
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
    public AddPermissionResponse addPermission(AddPermissionRequest addPermissionRequest)
            throws AmazonServiceException, AmazonClientException {

        return amazonSqsToBeExtended.addPermission(addPermissionRequest);
    }

    /**
     * <p>
     * Returns a list of your queues. The maximum number of queues that can be
     * returned is 1000. If you specify a value for the optional
     * <code>QueueNamePrefix</code> parameter, only queues with a name beginning
     * with the specified value are returned.
     * </p>
     *
     * @return The response from the ListQueues service method, as returned by
     *         AmazonSQS.
     *
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
    public ListQueuesResponse listQueues() throws AmazonServiceException, AmazonClientException {

        return amazonSqsToBeExtended.listQueues();
    }

    /** {@inheritDoc} */
    @Override public ListQueueTagsResponse listQueueTags(final ListQueueTagsRequest listQueueTagsRequest) {
        return amazonSqsToBeExtended.listQueueTags(listQueueTagsRequest);
    }

    /** {@inheritDoc} */
    @Override public TagQueueResponse tagQueue(final TagQueueRequest tagQueueRequest) {
        return amazonSqsToBeExtended.tagQueue(tagQueueRequest);
    }

    /** {@inheritDoc} */
    @Override public UntagQueueResponse untagQueue(final UntagQueueRequest untagQueueRequest) {
        return amazonSqsToBeExtended.untagQueue(untagQueueRequest);
    }

    @Override
    public String serviceName() {
        return amazonSqsToBeExtended.serviceName();
    }

    @Override
    public void close() {
        amazonSqsToBeExtended.close();
    }
}
