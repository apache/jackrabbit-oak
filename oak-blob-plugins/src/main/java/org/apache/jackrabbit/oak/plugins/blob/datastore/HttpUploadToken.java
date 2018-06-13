/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.plugins.blob.datastore;

import com.google.common.base.Joiner;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.jackrabbit.util.Base64;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Optional;

public class HttpUploadToken {
    private String blobId;
    private Optional<String> uploadId;

    public HttpUploadToken(@Nonnull String blobId, @Nullable String uploadId) {
        this.blobId = blobId;
        this.uploadId = Optional.ofNullable(uploadId);
    }

    public static HttpUploadToken fromEncodedToken(@Nonnull String encoded) {
        String[] parts = encoded.split("#", 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Encoded string is missing the signature");
        }

        String toBeDecoded = parts[0];
        String expectedSig = Base64.decode(parts[1]);
        String actualSig = new String(DigestUtils.sha256(toBeDecoded));
        if (!expectedSig.equals(actualSig)) {
            throw new IllegalArgumentException("Upload token signature does not match");
        }

        String decoded = Base64.decode(toBeDecoded);
        String decodedParts[] = decoded.split("#");
        if (decodedParts.length < 2) {
            throw new IllegalArgumentException("Not all upload token parts provided");
        }

        return new HttpUploadToken(decodedParts[0], decodedParts.length > 2 ? decodedParts[2] : null);
    }

    public String getEncodedToken() {
        String now = Instant.now().toString();
        String toBeEncoded = uploadId.isPresent() ?
                Joiner.on("#").join(blobId, now, uploadId.get()) :
                Joiner.on("#").join(blobId, now);
        String toBeSigned = Base64.encode(toBeEncoded);
        String sig = Base64.encode(new String(DigestUtils.sha256(toBeSigned)));
        return Joiner.on("#").join(toBeSigned, sig);
    }

    @Override
    public String toString() {
        return getEncodedToken();
    }

    public String getBlobId() {
        return blobId;
    }

    public Optional<String> getUploadId() {
        return uploadId;
    }
}
