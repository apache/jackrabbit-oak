/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.type;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.plugins.memory.GenericValue;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

/**
 * This class updates a Lucene index when node content is changed.
 */
public class DefaultTypeEditor implements CommitHook {

    @Override
    public NodeState processCommit(NodeState before, NodeState after)
            throws CommitFailedException {
        // TODO: Calculate default type from the node definition
        CoreValue defaultType =
                new GenericValue(PropertyType.NAME, "nt:unstructured");
        NodeBuilder builder = after.getBuilder();
        after.compareAgainstBaseState(
                before, new DefaultTypeDiff(builder, defaultType));
        return builder.getNodeState();
    }

    private static class DefaultTypeDiff extends DefaultNodeStateDiff {

        private final NodeBuilder builder;

        private final CoreValue defaultType;

        public DefaultTypeDiff(NodeBuilder builder, CoreValue defaultType) {
            this.builder = builder;
            this.defaultType = defaultType;
        }

        @Override
        public void childNodeAdded(String name, NodeState after) {
            if (!NodeStateUtils.isHidden(name)) {
                NodeBuilder childBuilder = builder.getChildBuilder(name);
                if (after.getProperty("jcr:primaryType") == null) {
                    childBuilder.setProperty("jcr:primaryType", defaultType);
                }
                DefaultTypeDiff childDiff =
                        new DefaultTypeDiff(childBuilder, defaultType);
                for (ChildNodeEntry entry : after.getChildNodeEntries()) {
                    childDiff.childNodeAdded(
                            entry.getName(), entry.getNodeState());
                }
            }
        }

        @Override
        public void childNodeChanged(
                String name, NodeState before, NodeState after) {
            if (!NodeStateUtils.isHidden(name)) {
                NodeBuilder childBuilder = builder.getChildBuilder(name);
                DefaultTypeDiff childDiff =
                        new DefaultTypeDiff(childBuilder, defaultType);
                after.compareAgainstBaseState(before, childDiff);
            }
        }

    }

}
