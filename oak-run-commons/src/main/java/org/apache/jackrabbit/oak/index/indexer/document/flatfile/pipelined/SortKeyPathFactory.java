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

import java.util.ArrayList;
import java.util.Set;

import static org.apache.jackrabbit.oak.commons.PathUtils.elements;

/**
 * Creates String[] representing a path efficiently. It implements the following optimizations:
 * <ul>
 * <li>Reuse an internal array list to accumulate the parts of the path, before creating a String[] sized to this particular path.
 * <li><a href="https://www.baeldung.com/string/intern">Interns</a> the top level elements of the path.
 * These are very likely to be repeated in most of the entries, so they are good candidates to be interned.
 * </ul>
 *
 */
public final class SortKeyPathFactory {
    private final ArrayList<String> arrayBuilder = new ArrayList<>(16);
    // Common words that appear in paths.
    // TODO: confirm that checking for the common words is a worthwhile optimization.
    private static final Set<String> commonWords = Set.of("content", "dam", "product-assets",
            "jcr:content", "jcr:title", "jcr:lastModified", "jcr:created", "jcr:primaryType", "jcr:uuid"
//            "cq:tags", "cq:lastModified", "cq:lastModifiedBy", "cq:template", "cq:templatePath",
//            "dc:format", "dc:title", "dc:description", "dc:creator", "dc:modified", "dc:created",
//            "dam:sha1", "dam:size", "dam:score", "dam:status", "dam:assetState", "dam:imported",
//            "usages", "predictedTags", "imageFeatures", "contentFragment", "pageTitle", "renditions",
//            "videoCodec", "audioCodec", "metadata", "original", "profile"
    );

    public String[] genSortKey(String path) {
        arrayBuilder.clear();
        int i = 0;
        for (String part : elements(path)) {
            // This first levels of the path will very likely be similar for most of the entries (e.g. /content/dam/<company>)
            // Interning these strings should provide a big reduction in memory usage.
            // It is not worth to intern all levels because at lower levels the names are more likely to be less diverse,
            // often even unique, so interning them would fill up the interned string hashtable with useless entries.
            if (i < 3 || part.length() == 1 || commonWords.contains(part)) {
                arrayBuilder.add(part.intern());
            } else {
                arrayBuilder.add(part);
            }
            i++;
        }
        return arrayBuilder.toArray(new String[0]);
    }
}
