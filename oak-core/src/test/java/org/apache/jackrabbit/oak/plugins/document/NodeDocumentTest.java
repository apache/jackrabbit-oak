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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.Comparator;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.revisionAreAmbiguous;
import static org.apache.jackrabbit.oak.plugins.document.Revision.RevisionComparator;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link NodeDocument}.
 */
public class NodeDocumentTest {

    @Test
    public void splitCollisions() throws Exception {
        MemoryDocumentStore docStore = new MemoryDocumentStore();
        String id = Utils.getPathFromId("/");
        NodeDocument doc = new NodeDocument(docStore);
        doc.put(Document.ID, id);
        UpdateOp op = new UpdateOp(id, false);
        for (int i = 0; i < NodeDocument.NUM_REVS_THRESHOLD + 1; i++) {
            Revision r = Revision.newRevision(1);
            NodeDocument.setRevision(op, r, "c");
            NodeDocument.addCollision(op, r);
        }
        UpdateUtils.applyChanges(doc, op, StableRevisionComparator.INSTANCE);
        doc.split(DummyRevisionContext.INSTANCE);
    }

    @Test
    public void ambiguousRevisions() {
        // revisions from same cluster node are not ambiguous
        RevisionContext context = DummyRevisionContext.INSTANCE;
        Revision r1 = new Revision(1, 0, 1);
        Revision r2 = new Revision(2, 0, 1);
        assertFalse(revisionAreAmbiguous(context, r1, r1));
        assertFalse(revisionAreAmbiguous(context, r1, r2));
        assertFalse(revisionAreAmbiguous(context, r2, r1));

        // revisions from different cluster nodes are not ambiguous
        // if seen with stable revision comparator
        r1 = new Revision(1, 0, 2);
        r2 = new Revision(2, 0, 1);
        assertFalse(revisionAreAmbiguous(context, r1, r1));
        assertFalse(revisionAreAmbiguous(context, r1, r2));
        assertFalse(revisionAreAmbiguous(context, r2, r1));

        // now use a revision comparator with seen-at support
        final RevisionComparator comparator = new RevisionComparator(1);
        context = new DummyRevisionContext() {
            @Override
            public Comparator<Revision> getRevisionComparator() {
                return comparator;
            }
        };
        r1 = new Revision(1, 0, 2);
        r2 = new Revision(2, 0, 1);
        // add revision to comparator in reverse time order
        comparator.add(r2, new Revision(2, 0, 0));
        comparator.add(r1, new Revision(3, 0, 0)); // r1 seen after r2
        assertFalse(revisionAreAmbiguous(context, r1, r1));
        assertFalse(revisionAreAmbiguous(context, r2, r2));
        assertTrue(revisionAreAmbiguous(context, r1, r2));
        assertTrue(revisionAreAmbiguous(context, r2, r1));
    }
}
