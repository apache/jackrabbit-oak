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

package org.apache.jackrabbit.oak.json;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.util.Base64;

import static com.google.common.base.Preconditions.checkArgument;

public class Base64BlobSerializer extends BlobSerializer implements BlobDeserializer {
    private static final int DEFAULT_LIMIT = Integer.getInteger("oak.serializer.maxBlobSize", (int)FileUtils.ONE_MB);
    private final int maxSize;

    public Base64BlobSerializer() {
        this(DEFAULT_LIMIT);
    }

    public Base64BlobSerializer(int maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public String serialize(Blob blob) {
        checkArgument(blob.length() < maxSize, "Cannot serialize Blob of size [%s] " +
                "which is more than allowed maxSize of [%s]", blob.length(), maxSize);
        try {
            try (InputStream is = blob.getNewStream()) {
                StringWriter writer = new StringWriter();
                Base64.encode(is, writer);
                return writer.toString();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Blob deserialize(String value) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        StringReader reader = new StringReader(value);
        try {
            Base64.decode(reader, baos);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new ArrayBasedBlob(baos.toByteArray());
    }
}
