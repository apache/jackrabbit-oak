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

package org.apache.jackrabbit.oak.plugins.document;

import java.util.Collection;
import java.util.Map;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.blob.ReferencedBlob;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;

public class BlobCollector {
    private final DocumentNodeStore nodeStore;

    public BlobCollector(DocumentNodeStore nodeStore) {
        this.nodeStore = nodeStore;
    }

    public void collect(NodeDocument doc, Collection<ReferencedBlob> blobs) {
        for (String key : doc.keySet()) {
            if (!Utils.isPropertyName(key)) {
                continue;
            }
            Map<Revision, String> valueMap = doc.getLocalMap(key);
            for (String v : valueMap.values()) {
                if (v != null) {
                    loadValue(v, blobs, doc.getPath());
                }
            }
        }
    }

    private void loadValue(String v, Collection<ReferencedBlob> blobs, String nodeId) {
        JsopReader reader = new JsopTokenizer(v);
        PropertyState p;
        if (reader.matches('[')) {
            p = DocumentPropertyState.readArrayProperty("x", nodeStore, reader);
            if (p.getType() == Type.BINARIES) {
                for (int i = 0; i < p.count(); i++) {
                    Blob b = p.getValue(Type.BINARY, i);
                    blobs.add(new ReferencedBlob(b, nodeId));
                }
            }
        } else {
            p = DocumentPropertyState.readProperty("x", nodeStore, reader);
            if (p.getType() == Type.BINARY) {
                Blob b = p.getValue(Type.BINARY);
                blobs.add(new ReferencedBlob(b, nodeId));
            }
        }
    }
}
