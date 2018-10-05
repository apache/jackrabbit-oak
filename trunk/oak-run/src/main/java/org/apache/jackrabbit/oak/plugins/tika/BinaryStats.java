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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Maps;
import org.codehaus.groovy.runtime.StringGroovyMethods;

import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;

class BinaryStats {
    private final TikaHelper tika;
    private final List<MimeTypeStats> stats;
    private long totalSize;
    private long totalCount;
    private long indexedSize;
    private long indexedCount;

    public BinaryStats(File tikaConfig, BinaryResourceProvider provider) throws IOException {
        this.tika = new TikaHelper(tikaConfig);
        this.stats = collectStats(provider);
    }

    public long getTotalSize() {
        return totalSize;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public long getIndexedSize() {
        return indexedSize;
    }

    public long getIndexedCount() {
        return indexedCount;
    }

    public String getSummary() throws IOException {
        return getSummary(stats);
    }

    private List<MimeTypeStats> collectStats(BinaryResourceProvider provider) throws IOException {
        Map<String, MimeTypeStats> stats = Maps.newHashMap();
        for (BinaryResource binary : provider.getBinaries("/")) {
            String mimeType = binary.getMimeType();
            if (mimeType != null) {
                MimeTypeStats mimeStats = stats.get(mimeType);
                if (mimeStats == null) {
                    mimeStats = createStat(mimeType);
                    stats.put(mimeType, mimeStats);
                }

                long size = binary.getByteSource().size();
                mimeStats.addSize(size);
                totalSize += size;
                totalCount++;

                if (mimeStats.isIndexed()) {
                    indexedSize += size;
                    indexedCount++;
                }
            }
        }

        List<MimeTypeStats> result = new ArrayList<MimeTypeStats>(stats.values());
        Collections.sort(result, Collections.reverseOrder());
        return result;
    }

    private String getSummary(List<MimeTypeStats> stats) {
        int maxWidth = 0;
        for (MimeTypeStats s : stats) {
            maxWidth = Math.max(maxWidth, s.getName().length());
        }

        maxWidth += 5;

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        pw.println("MimeType Stats");
        pw.printf("\tTotal size          : %s%n", humanReadableByteCount(totalSize));
        pw.printf("\tTotal indexed size  : %s%n", humanReadableByteCount(indexedSize));
        pw.printf("\tTotal count         : %d%n", totalCount);
        pw.printf("\tTotal indexed count : %d%n", indexedCount);
        pw.println();

        String header = center("Type", maxWidth) + " " +
                center("Indexed", 10) + " " +
                center("Supported", 10) + " " +
                center("Count", 10) + " " +
                center("Size", 10);

        pw.println(header);
        pw.println(Strings.repeat("_", header.length() + 5));

        for (MimeTypeStats s : stats) {
            pw.printf("%-" + maxWidth + "s|%10s|%10s|  %-8d|%10s%n",
                    s.getName(),
                    s.isIndexed(),
                    s.isSupported(),
                    s.getCount(),
                    humanReadableByteCount(s.getTotalSize()));
        }
        return sw.toString();
    }

    private MimeTypeStats createStat(String mimeType) {
        MimeTypeStats stats = new MimeTypeStats(mimeType);
        stats.setIndexed(tika.isIndexed(mimeType));
        stats.setSupported(tika.isSupportedMediaType(mimeType));
        return stats;
    }

    private static String center(String s, int width) {
        return StringGroovyMethods.center(s, width);
    }

    private static class MimeTypeStats implements Comparable<MimeTypeStats> {
        private final String mimeType;
        private int count;
        private long totalSize;
        private boolean supported;
        private boolean indexed;

        public MimeTypeStats(String mimeType) {
            this.mimeType = mimeType;
        }

        public void addSize(long size) {
            count++;
            totalSize += size;
        }

        public void setSupported(boolean supported) {
            this.supported = supported;
        }

        public void setIndexed(boolean indexed) {
            this.indexed = indexed;
        }

        public long getTotalSize() {
            return totalSize;
        }

        public int getCount() {
            return count;
        }

        public String getName() {
            return mimeType;
        }

        public boolean isIndexed() {
            return indexed;
        }

        public boolean isSupported() {
            return supported;
        }

        @Override
        public int compareTo(MimeTypeStats o) {
            return ComparisonChain.start()
                    .compareFalseFirst(indexed, o.indexed)
                    .compare(totalSize, o.totalSize)
                    .result();
        }
    }
}
