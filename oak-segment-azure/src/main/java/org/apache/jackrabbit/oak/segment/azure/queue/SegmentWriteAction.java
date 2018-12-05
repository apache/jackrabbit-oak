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
package org.apache.jackrabbit.oak.segment.azure.queue;

import org.apache.jackrabbit.oak.segment.azure.AzureSegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.Buffer;

import java.io.IOException;
import java.util.UUID;

public class SegmentWriteAction {

    private final AzureSegmentArchiveEntry indexEntry;

    private final byte[] buffer;

    private final int offset;

    private final int length;

    public SegmentWriteAction(AzureSegmentArchiveEntry indexEntry, byte[] buffer, int offset, int length) {
        this.indexEntry = indexEntry;

        this.buffer = new byte[length];
        for (int i = 0; i < length; i++) {
            this.buffer[i] = buffer[i + offset];
        }
        this.offset = 0;
        this.length = length;
    }

    public UUID getUuid() {
        return new UUID(indexEntry.getMsb(), indexEntry.getLsb());
    }

    public Buffer toBuffer() {
        return Buffer.wrap(buffer, offset, length);
    }

    void passTo(SegmentWriteQueue.SegmentConsumer consumer) throws IOException {
        consumer.consume(indexEntry, buffer, offset, length);
    }

    @Override
    public String toString() {
        return getUuid().toString();
    }
}
