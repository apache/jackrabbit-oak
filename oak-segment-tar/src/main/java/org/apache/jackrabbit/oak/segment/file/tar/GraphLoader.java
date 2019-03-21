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
package org.apache.jackrabbit.oak.segment.file.tar;

import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newHashMapWithExpectedSize;
import static org.apache.jackrabbit.oak.segment.file.tar.TarConstants.GRAPH_MAGIC;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.CRC32;

import org.apache.jackrabbit.oak.segment.spi.persistence.Buffer;
import org.apache.jackrabbit.oak.segment.util.ReaderAtEnd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GraphLoader {

    private static final Logger log = LoggerFactory.getLogger(GraphLoader.class);

    private static final int FOOTER_SIZE = 16;

    private GraphLoader() {
    }

    /**
     * Loads the optional pre-compiled graph entry from the given tar file.
     *
     * @return the graph or {@code null} if one was not found
     * @throws IOException if the tar file could not be read
     */
    public static Buffer loadGraph(ReaderAtEnd readerAtEnd) throws IOException {
        Buffer meta = readerAtEnd.readAtEnd(FOOTER_SIZE, FOOTER_SIZE);

        int crc32 = meta.getInt();
        int count = meta.getInt();
        int bytes = meta.getInt();
        int magic = meta.getInt();

        if (magic != GRAPH_MAGIC) {
            log.warn("Invalid graph magic number");
            return null;
        }

        if (count < 0) {
            log.warn("Invalid number of entries");
            return null;
        }

        if (bytes < 4 + count * 34) {
            log.warn("Invalid entry size");
            return null;
        }

        Buffer graph = readerAtEnd.readAtEnd(bytes, bytes);

        byte[] b = new byte[bytes - FOOTER_SIZE];

        graph.mark();
        graph.get(b);
        graph.reset();

        CRC32 checksum = new CRC32();
        checksum.update(b);

        if (crc32 != (int) checksum.getValue()) {
            log.warn("Invalid graph checksum in tar file");
            return null;
        }

        return graph;
    }

    public static Map<UUID, List<UUID>> parseGraph(Buffer buffer) {
        int nEntries = buffer.getInt(buffer.limit() - 12);

        Map<UUID, List<UUID>> graph = newHashMapWithExpectedSize(nEntries);

        for (int i = 0; i < nEntries; i++) {
            long msb = buffer.getLong();
            long lsb = buffer.getLong();
            int nVertices = buffer.getInt();

            List<UUID> vertices = newArrayListWithCapacity(nVertices);

            for (int j = 0; j < nVertices; j++) {
                long vMsb = buffer.getLong();
                long vLsb = buffer.getLong();
                vertices.add(new UUID(vMsb, vLsb));
            }

            graph.put(new UUID(msb, lsb), vertices);
        }

        return graph;
    }
}
