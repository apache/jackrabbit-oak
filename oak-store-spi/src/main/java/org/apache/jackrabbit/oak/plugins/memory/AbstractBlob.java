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
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.commons.properties.SystemPropertySupplier;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for {@link Blob} implementations.
 * This base class provides default implementations for
 * {@code hashCode} and {@code equals}.
 */
public abstract class AbstractBlob implements Blob {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractBlob.class);

    private static final boolean DEBUG_BLOB_EQUAL_LOG = SystemPropertySupplier
            .create("oak.abstractblob.equal.log", false)
            .loggingTo(LOG)
            .formatSetMessage( (name, value) -> String.format("%s set to: %s", name, value) )
            .get();

    private static final long DEBUG_BLOB_EQUAL_LOG_LIMIT = SystemPropertySupplier
            .create("oak.abstractblob.equal.log.limit", 100_000_000L)
            .loggingTo(LOG)
            .formatSetMessage( (name, value) -> String.format("%s set to: %s", name, value) )
            .get();

    public static boolean equal(Blob a, Blob b) {
        // shortcut: first compare lengths if known in advance
        long al = a.length();
        long bl = b.length();
        if (al != -1 && bl != -1 && al != bl) {
            return false; // blobs not equal, given known and non-equal lengths
        }

        String ai = a.getContentIdentity();
        String bi = b.getContentIdentity();

        //Check for identity first. If they are same then its
        //definitely same blob. If not we need to check further.
        if (ai != null && ai.equals(bi)){
            return true;
        }

        if (DEBUG_BLOB_EQUAL_LOG && al > DEBUG_BLOB_EQUAL_LOG_LIMIT) {
            LOG.debug("Blobs have the same length of {} and we're falling back to byte-wise comparison.", al);
        }

        try {
            try (InputStream ais = a.getNewStream(); InputStream bis = b.getNewStream()) {
                return contentEquals(ais, bis);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Blob equality check failed", e);
        }
    }

    private static boolean contentEquals(InputStream is1, InputStream is2) throws IOException {
        if (is1 == is2) {
            return true;
        }

        byte[] buf1 = new byte[4096];
        byte[] buf2 = new byte[4096];

        while (true) {
            // readNBytes handles "short reads" automatically, blocking until requested bytes or EOF
            int read1 = is1.readNBytes(buf1, 0, buf1.length);
            int read2 = is2.readNBytes(buf2, 0, buf2.length);

            if (read1 != read2) {
                return false; // Length mismatch
            }
            if (read1 == 0) {
                return true; // Reached EOF on both streams identically
            }
            // Arrays.equals is highly optimized via CPU intrinsics
            if (!Arrays.equals(buf1, 0, read1, buf2, 0, read2)) {
                return false; // Content mismatch
            }
        }
    }

    private ByteBuffer hashCode; // synchronized access

    protected AbstractBlob(ByteBuffer hashCode) {
        this.hashCode = hashCode;
    }

    protected AbstractBlob() {
        this(null);
    }

    private synchronized ByteBuffer getSha256() {
        // Blobs are immutable so we can safely cache the hash
        if (hashCode == null) {
            try {
                MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
                hashCode = ByteBuffer.wrap(sha256.digest(this.getNewStream().readAllBytes()));
            } catch (IOException e) {
                throw new IllegalStateException("Hash calculation failed", e);
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e);
            }
        }
        return hashCode;
    }

    //--------------------------------------------------------------< Blob >--

    @Override @Nullable
    public String getReference() {
        return null;
    }

    @Override
    public String getContentIdentity() {
        return null;
    }

//------------------------------------------------------------< Object >--

    /**
     * To {@code Blob} instances are considered equal iff they have the
     * same SHA-256 hash code or are equal.
     * @param other
     */
    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (other instanceof AbstractBlob) {
            AbstractBlob that = (AbstractBlob) other;
            // optimize the comparison if both this and the other blob
            // already have pre-computed SHA-256 hash codes
            synchronized (this) {
                if (hashCode != null) {
                    synchronized (that) {
                        if (that.hashCode != null) {
                            return hashCode.equals(that.hashCode);
                        }
                    }
                }
            }
        }

        return other instanceof Blob && equal(this, (Blob) other);
    }

    @Override
    public int hashCode() {
        return 0; // see Blob javadoc
    }

    @Override
    public String toString() {
        // https://www.baeldung.com/java-byte-arrays-hex-strings#using-thebiginteger-class
        // could use Java 17 HexFormat in the future
        byte[] bytes = getSha256().array();
        BigInteger bigInteger = new BigInteger(1, bytes);
        return String.format("%0" + (bytes.length << 1) + "x", bigInteger);
    }
}
