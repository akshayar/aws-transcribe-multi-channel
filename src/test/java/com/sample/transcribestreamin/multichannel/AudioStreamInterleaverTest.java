package com.sample.transcribestreamin.multichannel;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class AudioStreamInterleaverTest {
    InterleaveInputStream interleaveStream;

    public AudioStreamInterleaverTest(InputStream agentStream, InputStream callerStream) {
        interleaveStream = new InterleaveInputStream(agentStream, callerStream);
    }

    public static void main(String[] args) throws IOException {
        // Example usage
        byte[] agentData = new byte[]{1, 2, 3, 4};
        byte[] callerData = new byte[]{5, 6, 7, 8};
        byte[] out = new byte[4];

        ByteArrayInputStream agentStream = new ByteArrayInputStream(agentData);
        ByteArrayInputStream callerStream = new ByteArrayInputStream(callerData);
        InterleaveInputStream interleaveStream = new InterleaveInputStream(agentStream, callerStream);
        while (interleaveStream.read(out) > 0) {
            System.out.println(java.util.Arrays.toString(out)); // Output: [1, 5, 2, 6, 3, 7, 4, 8]
            out = new byte[4];
        }

    }
}
