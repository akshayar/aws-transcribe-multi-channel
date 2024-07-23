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
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.transcribestreaming.model.AudioStream;

import java.util.concurrent.ExecutorService;

/**
 * AudioStreamPublisher implements audio stream publisher.
 * AudioStreamPublisher emits audio stream asynchronously in a separate thread
 */
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class AudioStreamPublisher implements Publisher<AudioStream> {
    @Value("${chunkSizeInBytes:1024}")
    private int chunkSizeInBytes = 1024;
    @Autowired
    private ExecutorService executor ;
    private ByteToAudioEventSubscription.StreamReader streamReader;

    public AudioStreamPublisher(){}
    public AudioStreamPublisher(ByteToAudioEventSubscription.StreamReader reader) {
        this.streamReader=reader;
    }

    @Override
    public void subscribe(Subscriber<? super AudioStream> s) {
        Subscription subscription=new ByteToAudioEventSubscription(s,executor,chunkSizeInBytes, streamReader);
        s.onSubscribe(subscription);
    }

    public void setStreamReader(ByteToAudioEventSubscription.StreamReader streamReader) {
        this.streamReader = streamReader;
    }
}
