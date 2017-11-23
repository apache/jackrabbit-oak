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

package org.apache.jackrabbit.oak.plugins.document.rdb;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.oak.plugins.document.BlobReferenceIterator;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.QueryCondition;

public class RDBBlobReferenceIterator extends BlobReferenceIterator {

    private final RDBDocumentStore documentStore;

    public RDBBlobReferenceIterator(DocumentNodeStore nodeStore, RDBDocumentStore documentStore) {
        super(nodeStore);
        this.documentStore = documentStore;
    }

    private final static List<QueryCondition> WITH_BINARIES = Collections
            .singletonList(new QueryCondition(NodeDocument.HAS_BINARY_FLAG, "=", NodeDocument.HAS_BINARY_VAL));

    @Override
    public Iterator<NodeDocument> getIteratorOverDocsWithBinaries() {
        return this.documentStore
                .queryAsIterable(Collection.NODES, null, null, Collections.emptyList(), WITH_BINARIES, Integer.MAX_VALUE, null)
                .iterator();
    }
}
