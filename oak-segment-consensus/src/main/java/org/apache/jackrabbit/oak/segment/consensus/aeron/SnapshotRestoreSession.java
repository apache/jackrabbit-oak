/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.segment.consensus.aeron;

import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Stateful snapshot restore session.
 *
 * <p>Owns the frame-by-frame restore state so {@link SnapshotService} only has
 * to poll Aeron and delegate decoded fragments.</p>
 */
final class SnapshotRestoreSession implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SnapshotRestoreSession.class);

    private final File storeDirectory;
    private String head;
    private int epoch;
    private long timestamp;
    private int fileCount;
    private long leadershipTermId = -1L;
    private boolean metadataReceived;
    private FileReceiver currentFileReceiver;

    SnapshotRestoreSession(File storeDirectory) {
        this.storeDirectory = storeDirectory;
    }

    void onFragment(DirectBuffer buffer, int offset, int length) throws Exception {
        if (length < SimpleMessageHeader.ENCODED_LENGTH) {
            log.warn("⚠️  Snapshot fragment too short: {} bytes", length);
            return;
        }

        SimpleMessageHeader.HeaderInfo headerInfo = SimpleMessageHeader.decode(buffer, offset);
        if (headerInfo.templateId != SimpleMessageHeader.TEMPLATE_ID_SNAPSHOT) {
            return;
        }

        int payloadOffset = offset + SimpleMessageHeader.ENCODED_LENGTH;
        int maxPayloadLength = Math.max(0, length - SimpleMessageHeader.ENCODED_LENGTH);
        int payloadLength = Math.min(headerInfo.blockLength, maxPayloadLength);
        if (payloadLength == 0) {
            return;
        }

        byte[] payload = new byte[payloadLength];
        buffer.getBytes(payloadOffset, payload);
        processPayload(payload, payloadLength);
    }

    SnapshotService.SnapshotState complete() {
        closeCurrentReceiver();
        if (!metadataReceived) {
            log.warn("⚠️  Snapshot restoration incomplete: no metadata received");
            return null;
        }
        return new SnapshotService.SnapshotState(head, epoch, timestamp, fileCount, leadershipTermId);
    }

    @Override
    public void close() {
        closeCurrentReceiver();
    }

    private void processPayload(byte[] payload, int payloadLength) throws Exception {
        String payloadPreview = new String(
            payload,
            0,
            Math.min(200, payloadLength),
            StandardCharsets.UTF_8
        );

        if (payloadPreview.contains("\"type\":\"metadata\"")) {
            processMetadata(payload);
            return;
        }

        if (payloadPreview.contains("\"type\":\"file_header\"")) {
            processFileHeader(payload);
            return;
        }

        if (currentFileReceiver != null) {
            currentFileReceiver.writeChunk(payload, 0, payloadLength);
        } else {
            log.debug("Received snapshot chunk but no active receiver");
        }
    }

    private void processMetadata(byte[] payload) {
        String json = new String(payload, StandardCharsets.UTF_8);
        String parsedHead = extractJsonField(json, "head");
        Long epochValue = extractJsonFieldLong(json, "ethereumEpoch");
        if (epochValue == null) {
            epochValue = extractJsonFieldLong(json, "epoch");
        }
        Long timestampValue = extractJsonFieldLong(json, "timestamp");

        if (parsedHead == null || epochValue == null || timestampValue == null) {
            return;
        }

        this.head = parsedHead;
        this.epoch = epochValue.intValue();
        this.timestamp = timestampValue;
        Long termValue = extractJsonFieldLong(json, "leadershipTermId");
        this.leadershipTermId = termValue != null ? termValue : -1L;
        this.metadataReceived = true;
        log.info("   ✅ Metadata received: head={}, epoch={}", head, epoch);
    }

    private void processFileHeader(byte[] payload) throws Exception {
        String json = new String(payload, StandardCharsets.UTF_8);
        String fileType = extractJsonField(json, "fileType");
        String fileName = extractJsonField(json, "fileName");
        Long fileSize = extractJsonFieldLong(json, "fileSize");

        if (fileName == null || fileSize == null) {
            return;
        }

        closeCurrentReceiver();
        File targetFile = new File(storeDirectory, fileName);
        currentFileReceiver = new FileReceiver(targetFile, fileSize);
        fileCount++;
        log.info("   📥 Receiving {}: {} ({} bytes)", fileType, fileName, fileSize);
    }

    private void closeCurrentReceiver() {
        if (currentFileReceiver == null) {
            return;
        }
        currentFileReceiver.close();
        currentFileReceiver = null;
    }

    private static String extractJsonField(String json, String field) {
        String pattern = "\"" + field + "\":";
        int start = json.indexOf(pattern);
        if (start < 0) {
            return null;
        }

        start += pattern.length();
        while (start < json.length() && Character.isWhitespace(json.charAt(start))) {
            start++;
        }

        if (start >= json.length()) {
            return null;
        }

        if (json.charAt(start) == '"') {
            start++;
            int end = json.indexOf('"', start);
            if (end < 0) {
                return null;
            }
            return json.substring(start, end);
        }

        int end = start;
        while (end < json.length() && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-')) {
            end++;
        }
        return json.substring(start, end);
    }

    private static Long extractJsonFieldLong(String json, String field) {
        String value = extractJsonField(json, field);
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static final class FileReceiver implements AutoCloseable {
        private final File targetFile;
        private final long expectedSize;
        private long bytesReceived;
        private final FileOutputStream outputStream;

        FileReceiver(File targetFile, long expectedSize) throws IOException {
            this.targetFile = targetFile;
            this.expectedSize = expectedSize;
            File parent = targetFile.getParentFile();
            if (parent != null && !parent.exists() && !parent.mkdirs()) {
                throw new IOException("Failed to create parent directory for " + targetFile);
            }
            this.outputStream = new FileOutputStream(targetFile);
        }

        void writeChunk(byte[] data, int offset, int length) throws IOException {
            outputStream.write(data, offset, length);
            bytesReceived += length;
        }

        @Override
        public void close() {
            try {
                outputStream.close();
                if (bytesReceived == expectedSize) {
                    log.debug("✅ Snapshot file complete: {} ({} bytes)", targetFile.getName(), bytesReceived);
                } else {
                    log.warn(
                        "⚠️  Snapshot file size mismatch: {} (expected {}, got {})",
                        targetFile.getName(),
                        expectedSize,
                        bytesReceived
                    );
                }
            } catch (IOException e) {
                log.warn("❌ Failed to close snapshot file: {} - {}", targetFile.getName(), e.getMessage());
            }
        }
    }
}
