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

import org.apache.jackrabbit.oak.plugins.document.NodeDocument;

import java.util.HashMap;

class EndingStatistics {
    public int count = 0;
    public long size = 0;

    @Override
    public String toString() {
        return "{" +
                "count=" + count +
                ", size=" + size +
                '}';
    }
}

public class EndingsTracker {
    final HashMap<String, EndingStatistics> endings = new HashMap<>();

    public void trackDocument(NodeDocument next) {
        var id = getPath(next);
        var index = id.lastIndexOf("/jcr:content");
        if (index != -1) {
            var ending = id.substring(index);
            var predictedTagsIndex  = ending.indexOf("/metadata/predictedTags");
            if (predictedTagsIndex != -1)  {
                ending = ending.substring(0, predictedTagsIndex + "/metadata/predictedTags".length()) + "/*";
            }
            int docSize = (int) next.get(NodeDocumentCodec.SIZE_FIELD);
            var stats = endings.computeIfAbsent(ending, k -> new EndingStatistics());
            stats.count++;
            stats.size += docSize;
        }
    }

    private String getPath(NodeDocument next) {
        String p = (String) next.get(NodeDocument.PATH);
        if (p != null) {
            return p;
        }
        return next.getId();
    }
}
