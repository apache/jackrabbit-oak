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

import java.util.HashSet;
import java.util.Set;

/**
 * Test helper class that is capable to simply creating ClusterView and
 * ClusterViewDocument objs
 **/
class ClusterViewBuilder {

    private final Set<Integer> activeIds = new HashSet<Integer>();
    private final Set<Integer> recoveringIds = new HashSet<Integer>();
    private final Set<Integer> backlogIds = new HashSet<Integer>();
    private final Set<Integer> inactiveIds = new HashSet<Integer>();
    private final long viewSeqNum;
    private final int myId;

    ClusterViewBuilder(long viewSeqNum, int myId) {
        this.viewSeqNum = viewSeqNum;
        this.myId = myId;
    }

    public ClusterViewBuilder active(int... instanceIds) {
        for (int i = 0; i < instanceIds.length; i++) {
            int anId = instanceIds[i];
            activeIds.add(anId);
        }
        return this;
    }

    public ClusterViewBuilder recovering(int... instanceIds) {
        for (int i = 0; i < instanceIds.length; i++) {
            int anId = instanceIds[i];
            recoveringIds.add(anId);
        }
        return this;
    }

    public ClusterViewBuilder backlogs(int... instanceIds) {
        for (int i = 0; i < instanceIds.length; i++) {
            int anId = instanceIds[i];
            backlogIds.add(anId);
        }
        return this;
    }

    public ClusterViewBuilder inactive(int... instanceIds) {
        for (int i = 0; i < instanceIds.length; i++) {
            int anId = instanceIds[i];
            inactiveIds.add(anId);
        }
        return this;
    }

    public ClusterViewDocument asDoc() {
        /*
         * "_id" : "clusterView", "seqNum" : NumberLong(1), "inactive" : null,
         * "clusterViewHistory" : {
         * 
         * }, "deactivating" : null, "created" : "2015-06-30T08:21:29.393+0200",
         * "clusterViewId" : "882f8926-1112-493a-81a0-f946087b2986", "active" :
         * "1", "creator" : 1, "_modCount" : NumberLong(1)
         */
        Document doc = new Document();
        doc.put(ClusterViewDocument.VIEW_SEQ_NUM_KEY, viewSeqNum);
        doc.put(ClusterViewDocument.INACTIVE_KEY, asArrayStr(inactiveIds));
        doc.put(ClusterViewDocument.RECOVERING_KEY, asArrayStr(recoveringIds));
        doc.put(ClusterViewDocument.ACTIVE_KEY, asArrayStr(activeIds));
        ClusterViewDocument clusterViewDoc = new ClusterViewDocument(doc);
        return clusterViewDoc;
    }

    public ClusterView asView(String clusterId) {
        return ClusterView.fromDocument(myId, clusterId, asDoc(), backlogIds);
    }

    private String asArrayStr(Set<Integer> ids) {
        return ClusterViewDocument.arrayToCsv(asArray(ids));
    }

    private Integer[] asArray(Set<Integer> set) {
        if (set.size() == 0) {
            return null;
        } else {
            return set.toArray(new Integer[set.size()]);
        }
    }

}
