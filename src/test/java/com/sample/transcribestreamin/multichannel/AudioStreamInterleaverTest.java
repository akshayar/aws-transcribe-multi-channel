package com.sample.transcribestreamin.multichannel;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class AudioStreamInterleaverTest {
    private static final int BLOCK_SIZE = 2; // Assuming 2 bytes per sample
    InterleaveStream interleaveStream ;

    public AudioStreamInterleaverTest(InputStream agentStream, InputStream callerStream){
        interleaveStream=new InterleaveStream(agentStream, callerStream);
    }

    public static void main(String[] args) throws IOException {
        // Example usage
        byte[] agentData = new byte[] { 1, 2, 3, 4 };
        byte[] callerData = new byte[] { 5, 6, 7, 8 };
        byte[] out = new byte[4];

        ByteArrayInputStream agentStream = new ByteArrayInputStream(agentData);
        ByteArrayInputStream callerStream = new ByteArrayInputStream(callerData);
        InterleaveStream interleaveStream=new InterleaveStream(agentStream,callerStream);
        while (interleaveStream.read(out) >0){
            System.out.println(java.util.Arrays.toString(out)); // Output: [1, 5, 2, 6, 3, 7, 4, 8]
            out = new byte[4];
        }

    }
}
