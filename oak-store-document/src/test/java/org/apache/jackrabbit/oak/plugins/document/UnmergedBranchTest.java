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

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UnmergedBranchTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @Test
    public void purgeUnmergedBranch() throws Exception {
        DocumentStore testStore = new MemoryDocumentStore();
        DocumentMK mk1 = create(testStore, 1);
        int cId1 = mk1.getNodeStore().getClusterId();
        DocumentMK mk2 = create(testStore, 2);
        int cId2 = mk2.getNodeStore().getClusterId();

        //1. Create branch commits on both cluster nodes
        String rev1 = mk1.commit("", "+\"/child1\":{}", null, "");
        String branchRev1 = mk1.branch(rev1);
        String brev1 = mk1.commit("/child1", "^\"foo\":1", branchRev1, "");

        String rev2 = mk2.commit("", "+\"/child2\":{}", null, "");
        String branchRev2 = mk2.branch(rev2);
        String brev2 = mk2.commit("/child2", "^\"foo\":1", branchRev2, "");

        Map<Revision, RevisionVector> revs1 = getUncommittedRevisions(mk1);
        Map<Revision, RevisionVector> revs2 = getUncommittedRevisions(mk2);

        //2. Assert that branch rev are uncommited
        assertTrue(revs1.containsKey(RevisionVector.fromString(brev1).asTrunkRevision().getRevision(cId1)));
        assertTrue(revs2.containsKey(RevisionVector.fromString(brev2).asTrunkRevision().getRevision(cId2)));

        //3. Restart cluster 1 so that purge happens but only for cluster 1
        mk1.dispose();
        mk1 = create(testStore, 1);
        revs1 = getUncommittedRevisions(mk1);
        revs2 = getUncommittedRevisions(mk2);

        //4. Assert that post restart unmerged branch rev for c1 are purged
        assertFalse(revs1.containsKey(RevisionVector.fromString(brev1).asTrunkRevision().getRevision(cId1)));
        assertTrue(revs2.containsKey(RevisionVector.fromString(brev2).asTrunkRevision().getRevision(cId2)));

    }

    public SortedMap<Revision, RevisionVector> getUncommittedRevisions(DocumentMK mk) {
        // only look at revisions in this document.
        // uncommitted revisions are not split off
        NodeDocument doc = getRootDoc(mk);
        Map<Revision, String> valueMap = doc.getLocalMap(NodeDocument.REVISIONS);
        SortedMap<Revision, RevisionVector> revisions =
                new TreeMap<Revision, RevisionVector>(StableRevisionComparator.INSTANCE);
        for (Map.Entry<Revision, String> commit : valueMap.entrySet()) {
            if (!Utils.isCommitted(commit.getValue())) {
                Revision r = commit.getKey();
                if (r.getClusterId() == mk.getNodeStore().getClusterId()) {
                    RevisionVector b = RevisionVector.fromString(commit.getValue());
                    revisions.put(r, b);
                }
            }
        }
        return revisions;
    }

    private NodeDocument getRootDoc(DocumentMK mk){
        return mk.getNodeStore().getDocumentStore().find(Collection.NODES, Utils.getIdFromPath("/"));
    }

    private DocumentMK create(DocumentStore ds, int clusterId){
        return builderProvider.newBuilder().setAsyncDelay(0)
                .setDocumentStore(ds).setClusterId(clusterId).open();
    }
}
