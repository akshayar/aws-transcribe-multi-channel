/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.sample.transcribestreamin.multichannel;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.transcribestreaming.TranscribeStreamingAsyncClient;
import software.amazon.awssdk.services.transcribestreaming.model.AudioStream;
import software.amazon.awssdk.services.transcribestreaming.model.BadRequestException;
import software.amazon.awssdk.services.transcribestreaming.model.StartStreamTranscriptionRequest;
import software.amazon.awssdk.services.transcribestreaming.model.StartStreamTranscriptionResponseHandler;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;


/**
 * This class wraps the AWS SDK implementation of the AWS Transcribe API with some retry logic to handle common
 * error cases, such as flaky network connections.
 */
@Component
public class TranscribeStreamingRetryClient {
    private static final Logger LOG = LoggerFactory.getLogger(TranscribeStreamingRetryClient.class);

    private static final int DEFAULT_MAX_RETRIES = 10;
    private static final int DEFAULT_MAX_SLEEP_TIME_MILLS = 100;
    List<Class<?>> nonRetriableExceptions = List.of(BadRequestException.class);
    @Value("${maxRetries:10}")
    private int maxRetries = DEFAULT_MAX_RETRIES;
    @Value("${sleepTime:100}")
    private int sleepTime = DEFAULT_MAX_SLEEP_TIME_MILLS;
    @Autowired
    private TranscribeStreamingAsyncClient client;

    public TranscribeStreamingRetryClient() {

    }

    /**
     * Initiate TranscribeStreamingRetryClient with TranscribeStreamingAsyncClient
     *
     * @param client TranscribeStreamingAsyncClient
     */
    public TranscribeStreamingRetryClient(TranscribeStreamingAsyncClient client) {
        this.client = client;
    }

    /**
     * Get Max retries
     *
     * @return Max retries
     */
    public int getMaxRetries() {
        return maxRetries;
    }

    /**
     * Set Max retries
     *
     * @param maxRetries Max retries
     */
    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    /**
     * Get sleep time
     *
     * @return sleep time between retries
     */
    public int getSleepTime() {
        return sleepTime;
    }

    /**
     * Set sleep time between retries
     *
     * @param sleepTime sleep time
     */
    public void setSleepTime(int sleepTime) {
        this.sleepTime = sleepTime;
    }

    /**
     * Initiate a Stream Transcription with retry.
     *
     * @param request         StartStreamTranscriptionRequest to use to start transcription
     * @param publisher       The source audio stream as Publisher
     * @param responseHandler StreamTranscriptionBehavior object that defines how the response needs to be handled.
     * @return Completable future to handle stream response.
     */

    public CompletableFuture<Void> startStreamTranscription(final StartStreamTranscriptionRequest request,
                                                            final Publisher<AudioStream> publisher,
                                                            final StreamTranscriptionBehavior responseHandler) {

        CompletableFuture<Void> finalFuture = new CompletableFuture<>();

        recursiveStartStream(rebuildRequestWithSession(request), publisher, responseHandler, finalFuture, 0);

        return finalFuture;
    }

    /**
     * Recursively call startStreamTranscription() to be called till the request is completed or till we run out of retries.
     *
     * @param request         StartStreamTranscriptionRequest
     * @param publisher       The source audio stream as Publisher
     * @param responseHandler StreamTranscriptionBehavior object that defines how the response needs to be handled.
     * @param finalFuture     final future to finish on completing the chained futures.
     * @param retryAttempt    Current attempt number
     */
    private void recursiveStartStream(final StartStreamTranscriptionRequest request,
                                      final Publisher<AudioStream> publisher,
                                      final StreamTranscriptionBehavior responseHandler,
                                      final CompletableFuture<Void> finalFuture,
                                      final int retryAttempt) {
        CompletableFuture<Void> result = client.startStreamTranscription(request, publisher,
                getResponseHandler(responseHandler));
        result.whenComplete((r, e) -> {
            if (e != null) {

                if (retryAttempt <= maxRetries && isExceptionRetriable(e)) {
                    LOG.info("Retry attempt:" + (retryAttempt + 1));

                    try {
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                    recursiveStartStream(request, publisher, responseHandler, finalFuture, retryAttempt + 1);
                } else {
                    responseHandler.onError(e);
                    finalFuture.completeExceptionally(e);
                }
            } else {
                responseHandler.onComplete();
                finalFuture.complete(null);
            }
        });
    }

    private StartStreamTranscriptionRequest rebuildRequestWithSession(StartStreamTranscriptionRequest request) {
        return request.toBuilder()
                .sessionId(UUID.randomUUID().toString())
                .build();
    }

    /**
     * StartStreamTranscriptionResponseHandler implements subscriber of transcript stream
     * Output is printed to standard output
     */
    private StartStreamTranscriptionResponseHandler getResponseHandler(
            StreamTranscriptionBehavior transcriptionBehavior) {
        final StartStreamTranscriptionResponseHandler build = StartStreamTranscriptionResponseHandler.builder()
                .onResponse(r -> {
                    transcriptionBehavior.onResponse(r);
                })
                .onError(e -> {
                    //Do nothing here. Make sure you don't close any streams that should not be cleaned up yet.
                })
                .onComplete(() -> {
                    //Do nothing here. Make sure you don't close any streams that should not be cleaned up yet.
                })

                .subscriber(event -> transcriptionBehavior.onStream(event))
                .build();
        return build;
    }

    /**
     * Check if the exception is retriable or not.
     *
     * @param e Exception that occurred
     * @return True if the exception is retriable
     */
    private boolean isExceptionRetriable(Throwable e) {
        e.printStackTrace();
        return !nonRetriableExceptions.contains(e.getClass());
    }

    public void close() {
        this.client.close();
    }


}
