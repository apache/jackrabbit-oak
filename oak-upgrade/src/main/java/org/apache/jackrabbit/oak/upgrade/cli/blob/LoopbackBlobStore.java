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

package org.apache.jackrabbit.oak.upgrade.cli.blob;

import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Utility BlobStore implementation to be used in tooling that can work with a
 * FileStore without the need of the DataStore being present locally.
 *
 * Additionally instead of failing it tries to mimic and return blob reference
 * passed in by <b>caller</b> by passing it back as a binary.
 *
 * Example: requesting <code>blobId = e7c22b994c59d9</code> it will return the
 * <code>e7c22b994c59d9</code> text as a UTF-8 encoded binary file.
 */
public class LoopbackBlobStore implements BlobStore {

    @Override
    public String writeBlob(InputStream in) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String writeBlob(InputStream in, BlobOptions options) throws IOException {
        return writeBlob(in);
    }

    @Override
    public int readBlob(String blobId, long pos, byte[] buff, int off,
            int length) {
        // Only a part of binary can be requested!
        final int binaryLength = blobId.length();
        checkBinaryOffsetInRange(pos, binaryLength);
        final int effectiveSrcPos = Math.toIntExact(pos);
        final int effectiveBlobLengthToBeRead = Math.min(
                binaryLength - effectiveSrcPos, length);
        checkForBufferOverflow(buff, off, effectiveBlobLengthToBeRead);
        final byte[] blobIdBytes = getBlobIdStringAsByteArray(blobId);
        System.arraycopy(blobIdBytes, effectiveSrcPos, buff, off,
                effectiveBlobLengthToBeRead);
        return effectiveBlobLengthToBeRead;
    }

    private void checkForBufferOverflow(final byte[] buff, final int off,
                                        final int effectiveBlobLengthToBeRead) {
        if (buff.length < effectiveBlobLengthToBeRead + off) {
            // We cannot recover if buffer used to write is too small
            throw new UnsupportedOperationException("Edge case: cannot fit " +
                    "blobId in a buffer (buffer too small)");
        }
    }

    private void checkBinaryOffsetInRange(final long pos, final int binaryLength) {
        if (pos > binaryLength) {
            throw new IllegalArgumentException(
                    String.format("Offset %d out of range of %d", pos,
                            binaryLength));
        }
    }

    private byte[] getBlobIdStringAsByteArray(final String blobId) {
        return blobId.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public long getBlobLength(String blobId) throws IOException {
        return blobId.length();
    }

    @Override
    public InputStream getInputStream(String blobId) throws IOException {
        checkNotNull(blobId);
        return new ByteArrayInputStream(getBlobIdStringAsByteArray(blobId));
    }

    @Override
    public String getBlobId(@NotNull String reference) {
        return checkNotNull(reference);
    }

    @Override
    public String getReference(@NotNull String blobId) {
        return checkNotNull(blobId);
    }
}
