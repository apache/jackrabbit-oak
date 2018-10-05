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

package org.apache.jackrabbit.oak.plugins.document.bundlor;

import java.util.Collection;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.plugins.document.AbstractDocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class BundledDocumentDiffer {
    private final DocumentNodeStore nodeStore;

    public BundledDocumentDiffer(DocumentNodeStore nodeStore) {
        this.nodeStore = nodeStore;
    }

    /**
     * Performs diff for bundled nodes. The passed state can be DocumentNodeState or
     * one from secondary nodestore i.e. {@code DelegatingDocumentNodeState}. So the
     * passed states cannot be cast down to DocumentNodeState
     *
     * @param from from state
     * @param to to state
     * @param w jsop diff
     * @return true if the diff needs to be continued. In case diff is complete it would return false
     */
    public boolean diff(AbstractDocumentNodeState from, AbstractDocumentNodeState to, JsopWriter w){
        boolean fromBundled = BundlorUtils.isBundledNode(from);
        boolean toBundled = BundlorUtils.isBundledNode(to);

        //Neither of the nodes bundled
        if (!fromBundled && !toBundled){
            return true;
        }

        DocumentNodeState fromDocState = getDocumentNodeState(from);
        DocumentNodeState toDocState = getDocumentNodeState(to);

        diffChildren(fromDocState.getBundledChildNodeNames(), toDocState.getBundledChildNodeNames(), w);

        //If all child nodes are bundled then diff is complete
        if (fromDocState.hasOnlyBundledChildren()
                && toDocState.hasOnlyBundledChildren()){
            return false;
        }

        return true;
    }

    void diffChildren(Collection<String> fromChildren, Collection<String> toChildren, JsopWriter w){
        for (String n : fromChildren){
            if (!toChildren.contains(n)){
                w.tag('-').value(n);
            } else {
                //As lastRev for bundled node is same as parent node and they differ it means
                //children "may" also diff
                w.tag('^').key(n).object().endObject();
            }
        }

        for (String n : toChildren){
            if (!fromChildren.contains(n)){
                w.tag('+').key(n).object().endObject();
            }
        }
    }

    private DocumentNodeState getDocumentNodeState(AbstractDocumentNodeState state) {
        DocumentNodeState result;

        //Shortcut - If already a DocumentNodeState use as it. In case of SecondaryNodeStore
        //it can be DelegatingDocumentNodeState. In that case we need to read DocumentNodeState from
        //DocumentNodeStore and then get to DocumentNodeState for given path

        if (state instanceof DocumentNodeState) {
            result = (DocumentNodeState) state;
        } else if (BundlorUtils.isBundledChild(state)) {
            //In case of bundle child determine the bundling root
            //and from there traverse down to the actual child node
            checkState(BundlorUtils.isBundledChild(state));
            String bundlingPath = state.getString(DocumentBundlor.META_PROP_BUNDLING_PATH);
            String bundlingRootPath = PathUtils.getAncestorPath(state.getPath(), PathUtils.getDepth(bundlingPath));
            DocumentNodeState bundlingRoot = nodeStore.getNode(bundlingRootPath, state.getLastRevision());
            result = (DocumentNodeState) NodeStateUtils.getNode(bundlingRoot, bundlingPath);
        } else {
            result = nodeStore.getNode(state.getPath(), state.getLastRevision());
        }

        checkNotNull(result, "Node at [%s] not found for fromRev [%s]", state.getPath(), state.getLastRevision());
        return result;
    }
}
