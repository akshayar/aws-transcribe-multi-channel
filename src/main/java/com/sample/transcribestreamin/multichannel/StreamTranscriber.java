package com.sample.transcribestreamin.multichannel;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.transcribestreaming.model.LanguageCode;
import software.amazon.awssdk.services.transcribestreaming.model.MediaEncoding;
import software.amazon.awssdk.services.transcribestreaming.model.StartStreamTranscriptionRequest;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Component
public class StreamTranscriber implements ApplicationContextAware {

    private ApplicationContext applicationContext;

    @Value("${sampleRate}")
    private static final int sample_rate = 28800;

    @Autowired
    private TranscribeStreamingRetryClient transcribeStreamingRetryClient;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    public AudioStreamPublisher getAudioStreamPublisher(ByteToAudioEventSubscription.StreamReader streamReader) throws IOException {
        AudioStreamPublisher publisherTwoChannels= applicationContext.getBean(AudioStreamPublisher.class);
        publisherTwoChannels.setStreamReader(streamReader);
        return publisherTwoChannels;
        //return new AudioStreamPublisherTwoChannels(streamReader);
    }
    public void transcribe(ByteToAudioEventSubscription.StreamReader streamReader) throws ExecutionException, InterruptedException, IOException {
        // Implementation for transcribing audio streams
        StartStreamTranscriptionRequest request = StartStreamTranscriptionRequest.builder()
                .languageCode(LanguageCode.EN_US.toString())
                .mediaEncoding(MediaEncoding.PCM)
                .mediaSampleRateHertz(sample_rate)
                .enableChannelIdentification(true)
                .numberOfChannels(2)
                .showSpeakerLabel(Boolean.TRUE)
                .build();

        AudioStreamPublisher publisherTwoChannels=getAudioStreamPublisher(streamReader);

        CompletableFuture<Void> result = transcribeStreamingRetryClient.startStreamTranscription(
                request,
                publisherTwoChannels,
                new StreamTranscriptionBehaviorImpl());

        /**
         * Synchronous wait for stream to close, and close client connection
         */
        result.get();
        transcribeStreamingRetryClient.close();
    }

}
