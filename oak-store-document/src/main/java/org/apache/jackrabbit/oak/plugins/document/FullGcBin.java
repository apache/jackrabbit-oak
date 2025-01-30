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

import org.slf4j.Logger;
import static org.slf4j.LoggerFactory.getLogger;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FullGcBin {
    private static final Logger LOG = getLogger(FullGcBin.class);
    private final DocumentStore documentStore;
    private boolean enabled;

    public FullGcBin(DocumentStore ds) {
        documentStore = ds;
    }

    public int remove(Map<String, Long> orphanOrDeletedRemovalMap) {
        if (orphanOrDeletedRemovalMap.isEmpty() || !addToBin(orphanOrDeletedRemovalMap)) {
            return 0;
        }

        // use remove() with the modified check to rule
        // out any further race-condition where this removal
        // races with a un-orphan/re-creation as a result of which
        // the node should now not be removed. The modified check
        // ensures a node would then not be removed
        // (and as a result the removedSize != map.size())
        return documentStore.remove(Collection.NODES, orphanOrDeletedRemovalMap);
    }

    public List<NodeDocument> findAndUpdate(List<UpdateOp> updateOpList) {
        LOG.info("Updating {} documents", updateOpList.size());
        if (updateOpList.isEmpty() || !addToBin(updateOpList)) {
            return Collections.emptyList();
        }
        return documentStore.findAndUpdate(Collection.NODES, updateOpList);
    }

    private boolean addToBin(Map<String, Long> orphanOrDeletedRemovalMap) {
        if (!enabled) {
            LOG.info("Bin is disabled, skipping adding delete candidate documents to bin");
            return true;
        }
        LOG.info("Adding {} delete candidate documents to bin", orphanOrDeletedRemovalMap.size());
        List<UpdateOp> docs = orphanOrDeletedRemovalMap.keySet().stream()
            .map(e -> new UpdateOp(e, true))
            .map(this::insertOp)
            .collect(Collectors.toList());
        try {
            return documentStore.create(Collection.SETTINGS, docs);
        } catch (Exception e) {
            LOG.error("Error while adding delete candidate documents to bin", e);
        }
        return false;
    }

    private boolean addToBin(List<UpdateOp> updateOpList) {
        if (!enabled) {
            LOG.info("Bin is disabled, skipping adding removed properties to bin");
            return true;
        }
        LOG.info("Adding {} removed properties to bin", updateOpList.size());
        List<UpdateOp> binOpList = updateOpList.stream().map(this::insertOp).collect(Collectors.toList());
        try {
            documentStore.createOrUpdate(Collection.SETTINGS, binOpList);
            return true;
        } catch (Exception e) {
            LOG.error("Error while adding removed properties to bin", e);
        }
        return false;
    }

    /**
     * Create an insert operation from the given update operation
     *
     * @param op the update operation
     * @return the insert operation
     */
    private UpdateOp insertOp(UpdateOp op) {
        UpdateOp insertOp = new UpdateOp("/bin/" + op.getId(), true);
        //copy removed properties to the new document
        op.getChanges().forEach((k, v) -> {
            if (v.type == UpdateOp.Operation.Type.REMOVE) {
                insertOp.set(k.getName(), "");
            }
        });
        //this property is used to track the time when the document was added to the bin
        //it can be used as a TTL index property to automatically remove the document after a certain time
        //see https://www.mongodb.com/docs/manual/core/index-ttl/#std-label-index-feature-ttl
        insertOp.set("_gcCollectedAt", Instant.now().toEpochMilli());
        return insertOp;
    }

    public void setEnabled(boolean value) {
        this.enabled = value;
    }
}
