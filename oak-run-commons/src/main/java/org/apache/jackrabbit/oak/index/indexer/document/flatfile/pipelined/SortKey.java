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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Set;

import static org.apache.jackrabbit.oak.commons.PathUtils.elements;

public final class SortKey implements Comparable<SortKey> {
    private static final Set<String> COMMON_PATH_WORDS = Set.of(
            ":index",
            "assets",
            "audit",
            "components",
            "content",
            "dam",
            "data",
            "items",
            "jcr:content",
            "jcr:created",
            "jcr:primaryType",
            "jcr:system",
            "jcr:uuid",
            "jcr:versionStorage",
            "libs",
            "master",
            "metadata",
            "oak:index",
            "predictedTags",
            "product-assets",
            "related",
            "renditions",
            "uuid",
            "var"
    );

    public static String[] genSortKeyPathElements(String path) {
        String[] pathElements = new String[PathUtils.getDepth(path)];
        int i = 0;
        for (String part : elements(path)) {
            // This first levels of the path will very likely be similar for most of the entries (e.g. /content/dam/<company>)
            // Interning these strings should provide a big reduction in memory usage.
            // It is not worth to intern all levels because at lower levels the names are more likely to be less diverse,
            // often even unique, so interning them would fill up the interned string hashtable with useless entries.
            if (i < 3 || part.length() == 1 || COMMON_PATH_WORDS.contains(part)) {
                pathElements[i] = part.intern();
            } else {
                pathElements[i] = part;
            }
            i++;
        }
        return pathElements;
    }

    private final String[] pathElements;
    private final int bufferPos;

    public SortKey(String[] pathElements, int bufferPos) {
        this.pathElements = pathElements;
        this.bufferPos = bufferPos;
    }

    public int getBufferPos() {
        return bufferPos;
    }

    public String[] getPathElements() {
        return pathElements;
    }

    @Override
    public int compareTo(@NotNull SortKey o) {
        String[] p1 = this.pathElements;
        String[] p2 = o.pathElements;

        int i1 = 0;
        int i2 = 0;

        //Shorter paths come first i.e. first parent then children
        while (i1 < p1.length || i2 < p2.length) {
            if (i1 >= p1.length) {
                return -1;
            }
            if (i2 >= p2.length) {
                return 1;
            }

            String pe1 = p1[i1];
            i1++;
            String pe2 = p2[i2];
            i2++;

            int compare = pe1.compareTo(pe2);
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }
}
