package com.sample.transcribestreamin.multichannel;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Component
public class StreamTranscriber implements ApplicationContextAware {

    private ApplicationContext applicationContext;

    @Autowired
    private TranscribeStreamingRetryClient transcribeStreamingRetryClient;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    public AudioStreamPublisher getAudioStreamPublisher(ByteToAudioEventSubscription.StreamReader streamReader) throws IOException {
        AudioStreamPublisher publisherTwoChannels = applicationContext.getBean(AudioStreamPublisher.class);
        publisherTwoChannels.setStreamReader(streamReader);
        return publisherTwoChannels;
        //return new AudioStreamPublisherTwoChannels(streamReader);
    }

    public CompletableFuture<Void> transcribe(ByteToAudioEventSubscription.StreamReader streamReader) throws ExecutionException, InterruptedException, IOException {
        // Implementation for transcribing audio streams
        AudioStreamPublisher publisherTwoChannels = getAudioStreamPublisher(streamReader);

        return transcribeStreamingRetryClient.startStreamTranscription(
                streamReader.getTranscriptionRequest(),
                publisherTwoChannels,
                new StreamTranscriptionBehaviorImpl(streamReader.label()));
    }

}
