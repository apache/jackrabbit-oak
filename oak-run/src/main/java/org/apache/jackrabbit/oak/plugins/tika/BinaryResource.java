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

package org.apache.jackrabbit.oak.plugins.tika;

import com.google.common.io.ByteSource;

import static com.google.common.base.Preconditions.checkNotNull;

import org.jetbrains.annotations.Nullable;

class BinaryResource {
    private final ByteSource byteSource;
    private final String mimeType;
    private final String encoding;
    private final String path;
    private final String blobId;

    public BinaryResource(ByteSource byteSource,
                          @Nullable String mimeType,
                          @Nullable String encoding,
                          String path,
                          String blobId) {
        this.byteSource = checkNotNull(byteSource, "ByteSource must be provided");
        this.mimeType = mimeType;
        this.encoding = encoding;
        this.path = checkNotNull(path, "Path must be provided");
        this.blobId = checkNotNull(blobId, "BlobId must be specified");
    }

    public ByteSource getByteSource() {
        return byteSource;
    }

    @Nullable
    public String getMimeType() {
        return mimeType;
    }

    @Nullable
    public String getEncoding() {
        return encoding;
    }

    public String getPath() {
        return path;
    }

    public String getBlobId() {
        return blobId;
    }

    @Override
    public String toString() {
        return path;
    }
}
