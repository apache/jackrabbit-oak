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
package org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Optional;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.google.common.base.Joiner;
import org.apache.jackrabbit.util.Base64;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents an upload token returned by
 * {@link DataRecordDirectAccessProvider#initiateDirectUpload(long, int)} and
 * used in subsequent calls to {@link
 * DataRecordDirectAccessProvider#completeDirectUpload(String)}.  This class
 * handles creation, signing, and parsing of the token and uses the {@link
 * org.apache.jackrabbit.core.data.DataStore}’s secret key to sign the contents
 * of the token and to validate contents of tokens.
 */
public class DataRecordDirectUploadToken {
    private static Logger LOG = LoggerFactory.getLogger(DataRecordDirectUploadToken.class);

    private String blobId;
    private Optional<String> uploadId;

    public DataRecordDirectUploadToken(@NotNull String blobId, @Nullable String uploadId) {
        this.blobId = blobId;
        this.uploadId = Optional.ofNullable(uploadId);
    }

    public static DataRecordDirectUploadToken fromEncodedToken(@NotNull String encoded, @NotNull byte[] secret) {
        String[] parts = encoded.split("#", 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Encoded string is missing the signature");
        }

        String toBeDecoded = parts[0];
        String expectedSig = Base64.decode(parts[1]);
        String actualSig = getSignedString(toBeDecoded, secret);
        if (!expectedSig.equals(actualSig)) {
            throw new IllegalArgumentException("Upload token signature does not match");
        }

        String decoded = Base64.decode(toBeDecoded);
        String decodedParts[] = decoded.split("#");
        if (decodedParts.length < 2) {
            throw new IllegalArgumentException("Not all upload token parts provided");
        }

        return new DataRecordDirectUploadToken(decodedParts[0], decodedParts.length > 2 ? decodedParts[2] : null);
    }

    public String getEncodedToken(@NotNull byte[] secret) {
        String now = Instant.now().toString();
        String toBeEncoded = uploadId.isPresent() ?
                Joiner.on("#").join(blobId, now, uploadId.get()) :
                Joiner.on("#").join(blobId, now);
        String toBeSigned = Base64.encode(toBeEncoded);

        String sig = getSignedString(toBeSigned, secret);
        return sig != null ? Joiner.on("#").join(toBeSigned, sig) : toBeSigned;
    }

    private static String getSignedString(String toBeSigned, byte[] secret) {
        try {
            final String algorithm = "HmacSHA1";
            Mac mac = Mac.getInstance(algorithm);
            mac.init(new SecretKeySpec(secret, algorithm));
            byte[] hash = mac.doFinal(toBeSigned.getBytes());
            return new String(hash);
        }
        catch (NoSuchAlgorithmException | InvalidKeyException e) {
            LOG.warn("Could not sign upload token", e);
        }
        return null;
    }

    public String getBlobId() {
        return blobId;
    }

    public Optional<String> getUploadId() {
        return uploadId;
    }
}
