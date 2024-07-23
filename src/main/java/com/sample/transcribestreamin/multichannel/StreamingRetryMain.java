// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.sample.transcribestreamin.multichannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.transcribestreaming.TranscribeStreamingAsyncClient;

import javax.sound.sampled.LineUnavailableException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// snippet-start:[transcribe.java-streaming-retry-app]
@SpringBootApplication
public class StreamingRetryMain implements CommandLineRunner, ApplicationContextAware {
    private static Logger LOG = LoggerFactory.getLogger(StreamingRetryMain.class);
    @Value("${region}")
    private static final Region region = Region.AP_SOUTH_1;
    @Autowired
    private StreamTranscriber streamTranscriber;


    public static void main(String args[]) throws URISyntaxException, ExecutionException, InterruptedException,
            LineUnavailableException, FileNotFoundException {
        LOG.info("STARTING THE APPLICATION");
        SpringApplication.run(StreamingRetryMain.class, args);
        LOG.info("APPLICATION FINISHED");
    }


    @Override
    public void run(String... args) throws Exception {
        LOG.info("EXECUTING : command line runner");
        final String FILE2 =  "src/test/resources/speech_ai.wav";
        final String FILE1 = "src/test/resources/speech_nature.wav";

        InputStream streamOne=TranscribeHelper.getStreamFromFile(FILE1);
        InputStream streamTwo=TranscribeHelper.getStreamFromFile(FILE2);
        ByteToAudioEventSubscription.StreamReader streamReader=new ByteToAudioEventSubscription.StreamReader() {
            final InterleaveStream stream=new InterleaveStream(streamOne,streamTwo);
            @Override
            public int read(byte[] b) throws IOException {
                return stream.read(b);
            }
        };
        streamTranscriber.transcribe(streamReader);

    }


    @Bean
    public TranscribeStreamingAsyncClient getStreamingClient(){
        return TranscribeHelper.getTranscriptionClient(region);
    }

    @Bean
    public ExecutorService getExecutorService(){
        return Executors.newSingleThreadExecutor();
    }


    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }
    private ApplicationContext applicationContext;


}