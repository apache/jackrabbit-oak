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
package org.apache.jackrabbit.oak.plugins.memory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import com.google.common.base.Optional;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.apache.jackrabbit.oak.api.Blob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for {@link Blob} implementations.
 * This base class provides default implementations for
 * {@code hashCode} and {@code equals}.
 */
public abstract class AbstractBlob implements Blob {
    private static final Logger log = LoggerFactory.getLogger(AbstractBlob.class);

    private Optional<HashCode> hashCode = Optional.absent();

    /**
     * This hash code implementation returns the hash code of the underlying stream
     * @return
     */
    @Override
    public int hashCode() {
        return calculateSha256().asInt();
    }

    @Override
    public byte[] sha256() {
        return calculateSha256().asBytes();
    }

    private HashCode calculateSha256() {
        // Blobs are immutable so we can safely cache the hash
        if (!hashCode.isPresent()) {
            InputStream is = getNewStream();
            try {
                try {
                    Hasher hasher = Hashing.sha256().newHasher();
                    byte[] buf = new byte[0x1000];
                    int r;
                    while((r = is.read(buf)) != -1) {
                        hasher.putBytes(buf, 0, r);
                    }
                    hashCode = Optional.of(hasher.hash());
                }
                catch (IOException e) {
                    log.warn("Error while hashing stream", e);
                }
            }
            finally {
                close(is);
            }
        }
        return hashCode.get();
    }

    /**
     * To {@code Blob} instances are considered equal iff they have the same SHA-256 hash code
     * are equal.
     * @param other
     * @return
     */
    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof Blob)) {
            return false;
        }

        Blob that = (Blob) other;
        return Arrays.equals(sha256(), that.sha256());
    }

    private static void close(InputStream s) {
        try {
            s.close();
        }
        catch (IOException e) {
            log.warn("Error while closing stream", e);
        }
    }
}
