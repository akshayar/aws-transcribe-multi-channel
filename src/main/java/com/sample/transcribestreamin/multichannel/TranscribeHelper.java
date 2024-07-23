package com.sample.transcribestreamin.multichannel;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.transcribestreaming.TranscribeStreamingAsyncClient;

import javax.sound.sampled.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

public class TranscribeHelper {
    public static AudioInputStream getStreamFromMic() throws LineUnavailableException {

        // Signed PCM AudioFormat with 16kHz, 16 bit sample size, mono
        int sampleRate = 16000;
        AudioFormat format = new AudioFormat(sampleRate, 16, 1, true, false);
        DataLine.Info info = new DataLine.Info(TargetDataLine.class, format);

        if (!AudioSystem.isLineSupported(info)) {
            System.out.println("Line not supported");
            System.exit(0);
        }

        TargetDataLine line = (TargetDataLine) AudioSystem.getLine(info);
        line.open(format);
        line.start();

        return new AudioInputStream(line);
    }

    public static InputStream getStreamFromFile(File audioFile) {
        try {
            return new FileInputStream(audioFile);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static InputStream getStreamFromFile(String audioFile) {
        return getStreamFromFile(new File(audioFile));
    }

    public static TranscribeStreamingAsyncClient getTranscriptionClient(Region region) {
        return TranscribeStreamingAsyncClient.builder()
                .region(region)
                .build();

    }

}
