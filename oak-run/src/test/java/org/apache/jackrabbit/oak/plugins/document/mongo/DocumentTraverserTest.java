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

package org.apache.jackrabbit.oak.plugins.document.mongo;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.jackrabbit.oak.plugins.document.AbstractDocumentStoreTest;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.util.CloseableIterable;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getPathFromId;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

public class DocumentTraverserTest extends AbstractDocumentStoreTest {

    public DocumentTraverserTest(DocumentStoreFixture dsf) {
        super(dsf);
    }

    @Test
    public void getAllDocuments() throws Exception{
        assumeTrue(ds instanceof MongoDocumentStore);
        ds.create(Collection.NODES, asList(
                newDocument("/a/b", 1),
                newDocument("/a/c", 1),
                newDocument("/d", 1)
        ));

        ds.invalidateCache();

        MongoDocumentTraverser traverser = new MongoDocumentTraverser((MongoDocumentStore) ds);
        traverser.disableReadOnlyCheck();
        CloseableIterable<NodeDocument> itr = traverser.getAllDocuments(Collection.NODES, id -> getPathFromId(id).startsWith("/a"));
        Set<String> paths = StreamSupport.stream(itr.spliterator(), false)
                .map(NodeDocument::getPath)
                .collect(Collectors.toSet());

        itr.close();
        assertThat(paths, containsInAnyOrder("/a/b", "/a/c"));


        assertNotNull(ds.getIfCached(Collection.NODES, Utils.getIdFromPath("/a/b")));
        assertNotNull(ds.getIfCached(Collection.NODES, Utils.getIdFromPath("/a/c")));

        // Excluded id should not be cached
        assertNull(ds.getIfCached(Collection.NODES, Utils.getIdFromPath("/d")));
    }

    private static UpdateOp newDocument(String path, long modified) {
        String id = Utils.getIdFromPath(path);
        UpdateOp op = new UpdateOp(id, true);
        op.set(NodeDocument.MODIFIED_IN_SECS, modified);
        return op;
    }
}
