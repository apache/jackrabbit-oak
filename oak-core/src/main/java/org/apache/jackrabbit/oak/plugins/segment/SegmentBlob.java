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
package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.base.Preconditions.checkNotNull;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;

public class SegmentBlob implements Blob {

    private final SegmentReader reader;

    private final RecordId recordId;

    SegmentBlob(SegmentReader reader, RecordId recordId) {
        this.reader = checkNotNull(reader);
        this.recordId = checkNotNull(recordId);
    }

    @Override @Nonnull
    public SegmentStream getNewStream() {
        return reader.readStream(recordId);
    }

    @Override
    public long length() {
        SegmentStream stream = getNewStream();
        try {
            return stream.getLength();
        } finally {
            stream.close();
        }
    }

    @Override
    public byte[] sha256() {
        SegmentStream stream = getNewStream();
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");

            byte[] buffer = new byte[1024];
            for (int n = stream.read(buffer); n != -1; n = stream.read(buffer)) {
                digest.update(buffer, 0, n);
            }

            return digest.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new UnsupportedOperationException("SHA256 not supported", e);
        } finally {
            stream.close();
        }
    }

    @Override
    public int compareTo(Blob that) {
        throw new UnsupportedOperationException();
    }

}
