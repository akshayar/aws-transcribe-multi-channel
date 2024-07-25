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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.transcribestreaming.model.AudioEvent;
import software.amazon.awssdk.services.transcribestreaming.model.AudioStream;
import software.amazon.awssdk.services.transcribestreaming.model.StartStreamTranscriptionRequest;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

/**
 * This is an example Subscription implementation that converts bytes read from an AudioStream into AudioEvents
 * that can be sent to the Transcribe service. It implements a simple demand system that will read chunks of bytes
 * from an input stream containing audio data
 *
 * To read more about how Subscriptions and reactive streams work, please see
 * https://github.com/reactive-streams/reactive-streams-jvm/blob/v1.0.2/README.md
 */
public class ByteToAudioEventSubscription implements Subscription {
    private static final Logger LOG = LoggerFactory.getLogger(ByteToAudioEventSubscription.class);
    private final int chunkSizeInBytes;
    private final ExecutorService executor ;
    private final Subscriber<? super AudioStream> subscriber;
    private final StreamReader streamReader;


    public ByteToAudioEventSubscription(Subscriber<? super AudioStream> s, ExecutorService executor, int chunkSizeInBytes, StreamReader streamReader) {
        LOG.info("Creating ByteToAudioEventSubscription :{}",streamReader.label());
        this.subscriber = s;
        this.executor=executor;
        this.chunkSizeInBytes=chunkSizeInBytes;
        this.streamReader=streamReader;
    }

    @Override
    public void request(long n) {
        if (n <= 0) {
            subscriber.onError(new IllegalArgumentException("Demand must be positive"));
        }

        //We need to invoke this in a separate thread because the call to subscriber.onNext(...) is recursive
        try{
            executor.submit(()->{this.sendNEvents(n);});
        }catch (Exception e){
            LOG.error("Exception while submitting task to executor :{}",streamReader.label(),e);
        }

    }

    private synchronized void sendNEvents(long n) {
        for(long index=0 ; index < n ; index++){
            try {
                    ByteBuffer audioBuffer = getNextEvent();
                    if (audioBuffer.remaining() > 0) {
                        AudioEvent audioEvent = audioEventFromBuffer(audioBuffer);
                        subscriber.onNext(audioEvent);
                    } else {
                        subscriber.onComplete();
                        break;
                    }
            } catch (Exception e) {
                LOG.error("Exception while reading and sending event:{}",streamReader.label(),e);
                subscriber.onError(e);
            }

        }
    }

    @Override
    public void cancel() {
        executor.shutdown();
    }

    private ByteBuffer getNextEvent() {
        ByteBuffer audioBuffer = null;
        byte[] audioBytes = new byte[chunkSizeInBytes];

        try {
            int len = streamReader.read(audioBytes);

            if (len <= 0) {
                audioBuffer = ByteBuffer.allocate(0);
            } else {
                audioBuffer = ByteBuffer.wrap(audioBytes, 0, len);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return audioBuffer;
    }

    private AudioEvent audioEventFromBuffer(ByteBuffer bb) {
        return AudioEvent.builder()
                .audioChunk(SdkBytes.fromByteBuffer(bb))
                .build();
    }

    public static interface StreamReader {
        int read(byte[] b) throws IOException;

        StartStreamTranscriptionRequest getTranscriptionRequest();

        void close() ;

        String label();

    }

}
