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
package org.apache.jackrabbit.oak.segment.remote;

import static java.lang.Boolean.getBoolean;

import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class RemoteUtilities {
    public static final boolean OFF_HEAP = getBoolean("access.off.heap");
    public static final String SEGMENT_FILE_NAME_PATTERN = "^([0-9a-f]{4})\\.([0-9a-f-]+)$";
    public static final int MAX_ENTRY_COUNT = 0x10000;
    public static final Comparator<String> ARCHIVE_INDEX_COMPARATOR = new ArchiveIndexComparator();

    private static final Pattern PATTERN = Pattern.compile(SEGMENT_FILE_NAME_PATTERN);


    private RemoteUtilities() {
    }

    public static String getSegmentFileName(RemoteSegmentArchiveEntry indexEntry) {
        return getSegmentFileName(indexEntry.getPosition(), indexEntry.getMsb(), indexEntry.getLsb());
    }

    public static String getSegmentFileName(long offset, long msb, long lsb) {
        return String.format("%04x.%s", offset, new UUID(msb, lsb));
    }

    public static UUID getSegmentUUID(@NotNull String segmentFileName) {
        Matcher m = PATTERN.matcher(segmentFileName);
        if (!m.matches()) {
            return null;
        }
        return UUID.fromString(m.group(2));
    }

    public static boolean isSegmentName(String name) {
        return null != name && PATTERN.matcher(name).matches();
    }

    private static class ArchiveIndexComparator implements Comparator<String> {
        final static Pattern indexPattern = Pattern.compile("[0-9]+");

        @Override
        public int compare(String archive1, String archive2) {
            Matcher matcher1 = indexPattern.matcher(archive1);
            int index1 = matcher1.find() ? Integer.parseInt(matcher1.group()) : 0;
            Matcher matcher2 = indexPattern.matcher(archive2);
            int index2 = matcher2.find() ? Integer.parseInt(matcher2.group()) : 0;
            return index1 - index2;
        }
    }
}
