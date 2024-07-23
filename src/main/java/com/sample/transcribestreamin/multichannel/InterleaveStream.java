package com.sample.transcribestreamin.multichannel;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class InterleaveStream extends InputStream {
    private final InputStream agentStream;
    private final InputStream callerStream;
    private static final int BLOCK_SIZE = 2; // Assuming 2 bytes per sample

    public InterleaveStream(InputStream agentStream, InputStream callerStream) {
        this.agentStream = agentStream;
        this.callerStream = callerStream;
    }

    @Override
    public int read(byte[] byteBuffer) throws IOException {
        byte[] agentBuffer = new byte[BLOCK_SIZE];
        byte[] callerBuffer = new byte[BLOCK_SIZE];
        FixedSizeByteArrayOutputStream combinedStream = new FixedSizeByteArrayOutputStream(byteBuffer);
        int bytesRead;
        while (byteBuffer.length - combinedStream.size() >= 2 * BLOCK_SIZE && (bytesRead = read(agentBuffer, callerBuffer)) != -1) {
            combinedStream.write(agentBuffer, 0, bytesRead);
            combinedStream.write(callerBuffer, 0, bytesRead);
            agentBuffer = new byte[BLOCK_SIZE];
            callerBuffer = new byte[BLOCK_SIZE];
        }
        return combinedStream.size();
    }

    private int read(byte[] agentBuffer, byte[] callerBuffer) throws IOException {
        int agentBytesRead = agentStream.read(agentBuffer);
        int callerBytesRead = callerStream.read(callerBuffer);

        if (agentBytesRead == -1 && callerBytesRead == -1) {
            return -1; // End of both streams
        }

        if (agentBytesRead == -1) {
            return callerBytesRead; // End of agent stream
        }

        if (callerBytesRead == -1) {
            return agentBytesRead; // End of caller stream
        }

        return Math.min(agentBytesRead, callerBytesRead); // Return the minimum number of bytes read
    }

    @Override
    public int read() throws IOException {
        throw new UnsupportedOperationException("read() is not supported");
    }

    public static class FixedSizeByteArrayOutputStream extends ByteArrayOutputStream {
        private final int maxSize;

        public FixedSizeByteArrayOutputStream(byte[] byteArr) {
            this.buf=byteArr;
            this.maxSize = byteArr.length;
        }

        @Override
        public synchronized void write(int b) {
            if (count + 1 > maxSize) {
                throw new RuntimeException("Buffer limit exceeded");
            }
            super.write(b);
        }

        @Override
        public synchronized void write(byte[] b, int off, int len) {
            if (count + len > maxSize) {
                throw new RuntimeException("Buffer limit exceeded");
            }
            super.write(b, off, len);
        }
    }
}
