/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.segment.azure.tar.io;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.AppendBlobClient;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.file.tar.IOWriteAccess;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class AzureIOWriteAccess extends AzureIOReadAccess implements IOWriteAccess {

    /*
     * Access to "writeBuffer" and "length" is synchronized on "writeBuffer".
     */
    private final Buffer writeBuffer = Buffer.allocate(4 * 1024 * 1024);

    private int length;

    private final WriteAccessController writeAccessController;

    private volatile AppendBlobClient writeClient;

    public AzureIOWriteAccess(String archiveName, BlobClient blobClient, WriteAccessController writeAccessController) {
        super(archiveName, blobClient);
        this.writeAccessController = writeAccessController;
        this.length = 0;
    }

    private void initIfNeeded() {
        if (writeClient == null) {
            writeClient = readClient.getAppendBlobClient();
            try {
                writeClient.create();
            } catch (BlobStorageException e) {
                if (e.getErrorCode() == BlobErrorCode.BLOB_ALREADY_EXISTS) {
                    throw new IllegalStateException("Cannot create append blob \"" + writeClient.getBlobName() + "\", it already exists", e);
                }
            }
        }
    }

    @Override
    public int write(byte[] data) throws IOException {
        return write(data, 0, data.length);
    }

    @Override
    public int write(byte[] data, int offset, int len) throws IOException {
        writeAccessController.checkWritingAllowed();
        initIfNeeded();
        synchronized (writeBuffer) {
            if (writeBuffer.remaining() <= len) {
                flush();
            }
            writeBuffer.put(data, offset, len);
            this.length += len;
        }
        return len;
    }

    @Override
    public Buffer read(int startOffsetInclusive, int len) throws IOException {
        int endOffsetExclusive = startOffsetInclusive + len;
        int persistedEndOffsetExclusive;
        synchronized (writeBuffer) {
            persistedEndOffsetExclusive = this.length - writeBuffer.position();
        }

        if (persistedEndOffsetExclusive >= endOffsetExclusive) {
            return super.read(startOffsetInclusive, len);
        }

        int bufferEndOffsetExclusive;
        byte[] copiedBytes;
        synchronized (writeBuffer) {
            // recompute persistedLength in case it changed since it was last computed
            persistedEndOffsetExclusive = this.length - writeBuffer.position();
            bufferEndOffsetExclusive = this.length;
            int bufferedLength = bufferEndOffsetExclusive - persistedEndOffsetExclusive;
            System.arraycopy(writeBuffer.array(), 0, copiedBytes = new byte[bufferedLength], 0, bufferedLength);
        }

        Buffer bytes = Buffer.allocate(len);
        if (startOffsetInclusive < persistedEndOffsetExclusive) {
            // read the part that is already persisted
            int readFromPersistence = persistedEndOffsetExclusive - startOffsetInclusive;
            bytes.put(super.read(startOffsetInclusive, readFromPersistence));
            int effectiveLength = len - readFromPersistence;
            try {
                bytes.put(copiedBytes, 0, effectiveLength);
            } catch (IndexOutOfBoundsException e) {
                throw new IllegalStateException("Unexpected end of write buffer: " + copiedBytes.length + " -> 0:" + effectiveLength, e);
            }
        } else {
            // read fully from write buffer
            int pos = startOffsetInclusive - persistedEndOffsetExclusive;
            try {
                bytes.put(copiedBytes, pos, len);
            } catch (IndexOutOfBoundsException e) {
                throw new IllegalStateException("Unexpected end of write buffer: " + copiedBytes.length + " -> " + pos + ":" + len, e);
            }
        }

        bytes.rewind();
        return bytes;
    }

    @Override
    public void flush() throws IOException {
        if (writeBuffer.position() > 0) {
            byte[] bytes = null;
            int offset = -1;
            synchronized (writeBuffer) {
                if (writeBuffer.position() > 0) {
                    writeAccessController.checkWritingAllowed();
                    int length = writeBuffer.position();
                    System.arraycopy(writeBuffer.array(), 0, bytes = new byte[length], 0, length);
                    writeBuffer.position(0);
                    offset = this.length;
                }
            }
            if (bytes != null && offset != -1) {
                writeClient.appendBlock(new ByteArrayInputStream(bytes), bytes.length);
            }
        }
    }

    @Override
    public boolean isDirty() {
        return writeClient != null;
    }

    @Override
    public int length() {
        synchronized (writeBuffer) {
            return this.length;
        }
    }

    @Override
    public void close() throws IOException {
        flush();
    }
}
