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
import org.apache.jackrabbit.oak.plugins.memory.AbstractBlob;

public class SegmentBlob extends AbstractBlob {

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
    public int compareTo(Blob blob) {
        if (blob instanceof SegmentBlob) {
            SegmentBlob that = (SegmentBlob) blob;
            if (recordId.equals(that.recordId)) {
                return 0;
            }
        }
        return super.compareTo(blob);
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof SegmentBlob) {
            SegmentBlob that = (SegmentBlob) object;
            if (recordId.equals(that.recordId)) {
                return true;
            }
        }
        return super.equals(object);
    }

}
