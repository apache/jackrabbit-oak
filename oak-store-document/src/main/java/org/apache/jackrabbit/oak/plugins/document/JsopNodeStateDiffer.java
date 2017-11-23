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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

/**
 * A {@link NodeStateDiffer} implementation backed by a JSOP String.
 */
class JsopNodeStateDiffer implements NodeStateDiffer {

    private final String jsonDiff;
    private boolean withoutPropertyChanges = false;

    JsopNodeStateDiffer(String diff) {
        this.jsonDiff = diff == null ? "" : diff;
    }

    JsopNodeStateDiffer withoutPropertyChanges() {
        withoutPropertyChanges = true;
        return this;
    }

    @Override
    public boolean compare(@Nonnull final AbstractDocumentNodeState node,
                           @Nonnull final AbstractDocumentNodeState base,
                           @Nonnull final NodeStateDiff diff) {
        if (!withoutPropertyChanges) {
            if (!AbstractNodeState.comparePropertiesAgainstBaseState(node, base, diff)) {
                return false;
            }
        }

        return DiffCache.parseJsopDiff(jsonDiff, new DiffCache.Diff() {
            @Override
            public boolean childNodeAdded(String name) {
                return diff.childNodeAdded(name,
                        node.getChildNode(name));
            }

            @Override
            public boolean childNodeChanged(String name) {
                boolean continueComparison = true;
                NodeState baseChild = base.getChildNode(name);
                NodeState nodeChild = node.getChildNode(name);
                if (baseChild.exists()) {
                    if (nodeChild.exists()) {
                        continueComparison = compareExisting(
                                baseChild, nodeChild, name, diff);
                    } else {
                        continueComparison = diff.childNodeDeleted(name,
                                baseChild);
                    }
                } else {
                    if (nodeChild.exists()) {
                        continueComparison = diff.childNodeAdded(name,
                                nodeChild);
                    }
                }
                return continueComparison;
            }

            @Override
            public boolean childNodeDeleted(String name) {
                return diff.childNodeDeleted(name,
                        base.getChildNode(name));
            }
        });
    }

    private static boolean compareExisting(NodeState baseChild,
                                           NodeState nodeChild,
                                           String name,
                                           NodeStateDiff diff) {
        if (baseChild instanceof AbstractDocumentNodeState
                && nodeChild instanceof AbstractDocumentNodeState) {
            AbstractDocumentNodeState beforeChild = (AbstractDocumentNodeState) baseChild;
            AbstractDocumentNodeState afterChild = (AbstractDocumentNodeState) nodeChild;
            if (beforeChild.getLastRevision().equals(afterChild.getLastRevision())) {
                return true;
            }
        }
        return diff.childNodeChanged(name, baseChild, nodeChild);
    }
}
