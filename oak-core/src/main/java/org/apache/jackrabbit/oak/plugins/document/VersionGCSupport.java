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

import java.util.List;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public class VersionGCSupport {
    private final DocumentStore store;

    public VersionGCSupport(DocumentStore store) {
        this.store = store;
    }

    public Iterable<NodeDocument> getPossiblyDeletedDocs(final long lastModifiedTime) {
        //Fetch all documents.
        List<NodeDocument> nodes = store.query(Collection.NODES,NodeDocument.MIN_ID_VALUE,
                NodeDocument.MAX_ID_VALUE, Integer.MAX_VALUE);
        return Iterables.filter(nodes, new Predicate<NodeDocument>() {
            @Override
            public boolean apply(NodeDocument input) {
                return input.wasDeletedOnce() && !input.hasBeenModifiedSince(lastModifiedTime);
            }
        });
    }
}
