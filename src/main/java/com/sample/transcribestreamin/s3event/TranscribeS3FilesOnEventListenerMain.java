package com.sample.transcribestreamin.s3event;

import ch.qos.logback.core.util.FileUtil;
import com.google.gson.Gson;
import com.sample.transcribestreamin.multichannel.ByteToAudioEventSubscription;
import com.sample.transcribestreamin.multichannel.InterleaveInputStream;
import com.sample.transcribestreamin.multichannel.StreamTranscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.FileSystemUtils;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.eventnotifications.s3.model.S3EventNotification;
import software.amazon.awssdk.eventnotifications.s3.model.S3EventNotificationRecord;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.transcribestreaming.model.LanguageCode;
import software.amazon.awssdk.services.transcribestreaming.model.MediaEncoding;
import software.amazon.awssdk.services.transcribestreaming.model.StartStreamTranscriptionRequest;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

@SpringBootApplication(scanBasePackages = "com.sample.transcribestreamin")
public class TranscribeS3FilesOnEventListenerMain {
    private static final Logger logger = LoggerFactory.getLogger(TranscribeS3FilesOnEventListenerMain.class);
    @Value("${file.stream.sampleRate}")
    private static final int sample_rate = 28800;
    private final Gson gson = new Gson();
    @Autowired
    StreamTranscriber streamTranscriber;
    @Autowired
    S3Client amazonS3;
    ConcurrentHashMap<String, TranscribeDetail> transcribeDetailConcurrentHashMap = new ConcurrentHashMap<>();
    @Value("${region:ap-south-1}")
    private Region region;
    @Autowired
    @Qualifier("s3SqsClient")
    private SqsClient sqsClient;
    @Value("${s3.sqsQueueUrl}")
    private String sqsQueueUrl;
    @Value("${event.listener.type}")
    private String eventListenerType;

    public static void main(String[] args) {
        org.springframework.boot.SpringApplication.run(TranscribeS3FilesOnEventListenerMain.class, args);
    }

    @Bean("s3SqsClient")
    public SqsClient sqsClient() {
        return SqsClient.builder()
                .region(region)
                .build();
    }

    @Bean
    public S3Client amazonS3() {
        return S3Client.builder().region(region).build();
    }

    @PostConstruct
    public void init() {
        while ("s3".equalsIgnoreCase(eventListenerType)) {
            pollMessages();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

    }

    private void pollMessages() {
        logger.info("Polling messages");
        try {
            // Receive messages from the queue
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(sqsQueueUrl)
                    .maxNumberOfMessages(10)
                    .waitTimeSeconds(20) // Enable long polling
                    .build();

            List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();

            // Process received messages
            for (Message message : messages) {
                String messageBody = message.body();
                logger.info(messageBody);
                S3EventNotification s3EventNotification = S3EventNotification.fromJson(messageBody);
                if (s3EventNotification != null) {
                    Optional.ofNullable(s3EventNotification.getRecords()).orElse(Collections.emptyList()).stream().forEach(this::submitTranscriptionNoException);

                    // Delete the message from the queue
                    DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                            .queueUrl(sqsQueueUrl)
                            .receiptHandle(message.receiptHandle())
                            .build();
                    sqsClient.deleteMessage(deleteRequest);
                }
            }
        } catch (Exception e) {
            logger.error("Error polling messages: " + e.getMessage(), e);
        }
    }

    public void submitTranscriptionNoException(S3EventNotificationRecord eventNotificationRecord) {
        try {
            submitTranscription(eventNotificationRecord);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void submitTranscription(S3EventNotificationRecord eventNotificationRecord) throws IOException, ExecutionException, InterruptedException {
        logger.info("Processing event: " + gson.toJson(eventNotificationRecord));
        String bucketName = eventNotificationRecord.getS3().getBucket().getName();
        String objectKey = eventNotificationRecord.getS3().getObject().getKey();
        Path objectPath = Paths.get("s3://" + bucketName + "/" + objectKey);
        Path parentPath = objectPath.getParent();
        logger.info("Bucket: {}, key: {}, parent:{}", bucketName, objectKey, parentPath);

        if (transcribeDetailConcurrentHashMap.containsKey(parentPath.toString())) {
            TranscribeDetail transcribeDetail = transcribeDetailConcurrentHashMap.get(parentPath.toString());
            transcribeDetail.objectKey2 = objectKey;
            copyToFile(bucketName, transcribeDetail.objectKey1);
            copyToFile(bucketName, transcribeDetail.objectKey2);
            transcribeDetail.reader = new S3FileTranscribeUpdatableReader(new FileInputStream(transcribeDetail.objectKey1), new FileInputStream(transcribeDetail.objectKey2));
            transcribeDetail.reader.startStreamTranscriptionRequest = StartStreamTranscriptionRequest.builder()
                    .languageCode(LanguageCode.EN_US.toString())
                    .mediaEncoding(MediaEncoding.PCM)
                    .mediaSampleRateHertz(sample_rate)
                    .enableChannelIdentification(true)
                    .numberOfChannels(2)
                    .showSpeakerLabel(Boolean.TRUE)
                    .build();
            transcribeDetail.reader.label = String.valueOf(parentPath);
            transcribeDetail.result = streamTranscriber.transcribe(transcribeDetail.reader);

            transcribeDetail.result.whenComplete((result, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                }
                transcribeDetailConcurrentHashMap.remove(parentPath.toString());
                logger.info("Transcription completed for: " + parentPath);
                deleteFile(transcribeDetail.objectKey1);
                deleteFile(transcribeDetail.objectKey2);
                transcribeDetail.reader.close();

            });
        } else {
            TranscribeDetail transcribeDetail = new TranscribeDetail();
            transcribeDetail.bucketName = bucketName;
            transcribeDetail.objectKey1 = objectKey;
            transcribeDetailConcurrentHashMap.put(parentPath.toString(), transcribeDetail);
        }

    }

    private void copyToFile(String bucketName, String objectKey) throws IOException {
        ResponseBytes<GetObjectResponse> s3Object1 = amazonS3.getObjectAsBytes(GetObjectRequest.builder().bucket(bucketName).key(objectKey).build());
        byte[] data = s3Object1.asByteArray();
        File file = new File(objectKey);
        file.getParentFile().mkdirs();
        file.createNewFile();
        FileCopyUtils.copy(data, file);
    }

    private void deleteFile(String objectKey) {
        File file = new File(objectKey);
        file.delete();
    }

    public static class S3FileTranscribeUpdatableReader implements ByteToAudioEventSubscription.StreamReader {

        public String label;
        InterleaveInputStream stream;
        StartStreamTranscriptionRequest startStreamTranscriptionRequest;
        boolean stopped = false;

        public S3FileTranscribeUpdatableReader(InputStream i1, InputStream i2) {
            stream = new InterleaveInputStream(i1, i2);
        }

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
            return startStreamTranscriptionRequest;
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
            return label;
        }

    }

    public static class TranscribeDetail {
        public S3FileTranscribeUpdatableReader reader;
        public CompletableFuture<Void> result;
        public String bucketName, objectKey1, objectKey2;

    }

}
