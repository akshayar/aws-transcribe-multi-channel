// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.sample.transcribestreamin.file.local;

import com.sample.transcribestreamin.multichannel.ByteToAudioEventSubscription;
import com.sample.transcribestreamin.multichannel.InterleaveInputStream;
import com.sample.transcribestreamin.multichannel.StreamTranscriber;
import com.sample.transcribestreamin.multichannel.TranscribeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.transcribestreaming.TranscribeStreamingAsyncClient;
import software.amazon.awssdk.services.transcribestreaming.model.LanguageCode;
import software.amazon.awssdk.services.transcribestreaming.model.MediaEncoding;
import software.amazon.awssdk.services.transcribestreaming.model.StartStreamTranscriptionRequest;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// snippet-start:[transcribe.java-streaming-retry-app]
@SpringBootApplication(scanBasePackages = "com.sample.transcribestreamin.multichannel,com.sample.transcribestreamin.file.local")
public class TranscribeStreamingTwoFilesMain implements CommandLineRunner, ApplicationContextAware {
    private static final Logger LOG = LoggerFactory.getLogger(TranscribeStreamingTwoFilesMain.class);
    @Value("${region}")
    private static final Region region = Region.AP_SOUTH_1;
    @Value("${file.stream.sampleRate}")
    private static final int sample_rate = 28800;
    @Autowired
    private StreamTranscriber streamTranscriber;
    private ApplicationContext applicationContext;

    public static void main(String[] args) {
        LOG.info("STARTING THE APPLICATION");
        SpringApplication.run(TranscribeStreamingTwoFilesMain.class, args);
        LOG.info("APPLICATION FINISHED");
    }

    @Bean
    public TranscribeStreamingAsyncClient getStreamingClient() {
        return TranscribeStreamingAsyncClient.builder()
                .region(region)
                .build();
    }

    @Bean(name = "transcriptionExecutorService")
    @Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public ExecutorService getExecutorService() {
        return Executors.newSingleThreadExecutor();
    }

    @Bean
    public AwsCredentialsProvider credentialsProvider() {
        return DefaultCredentialsProvider.builder().build();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Override
    public void run(String... args) throws Exception {
        LOG.info("EXECUTING : command line runner"+ Arrays.asList(args));
        String FILE2 = "src/test/resources/speech_ai.wav";
        String FILE1 = "src/test/resources/speech_nature.wav";
        if(args.length != 0){
            FILE1=args[0];
            FILE2=args[1];
        }


        InputStream streamOne = TranscribeHelper.getStreamFromFile(FILE1);
        InputStream streamTwo = TranscribeHelper.getStreamFromFile(FILE2);

        ByteToAudioEventSubscription.StreamReader streamReader = new ByteToAudioEventSubscription.StreamReader() {
            final InterleaveInputStream stream = new InterleaveInputStream(streamOne, streamTwo);
            boolean stopped = false;

            @Override
            public int read(byte[] b) throws IOException {
                if (!stopped) {
                    return stream.read(b);
                } else {
                    return -1;
                }

            }

            @Override
            public StartStreamTranscriptionRequest getTranscriptionRequest() {
                return StartStreamTranscriptionRequest.builder()
                        .languageCode(LanguageCode.EN_US.toString())
                        .mediaEncoding(MediaEncoding.PCM)
                        .mediaSampleRateHertz(sample_rate)
                        .enableChannelIdentification(true)
                        .numberOfChannels(2)
                        .showSpeakerLabel(Boolean.TRUE)
                        .build();
            }

            @Override
            public void close() {
                try {
                    stopped = true;
                    stream.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public String label() {
                return "twoFile";
            }

        };
        CompletableFuture<Void> result = streamTranscriber.transcribe(streamReader);
        result.get();
        LOG.info("DONE");
        System.exit(0);

    }

}