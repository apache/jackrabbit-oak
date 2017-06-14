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
package org.apache.jackrabbit.oak.spi.commit;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler.Resolution;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class ConflictHandlers {

    private ConflictHandlers() {
    }

    @SuppressWarnings("deprecation")
    public static ThreeWayConflictHandler wrap(PartialConflictHandler handler) {
        return new ThreeWayConflictHandlerWrapper(handler);
    }

    @SuppressWarnings("deprecation")
    private static Resolution wrap(org.apache.jackrabbit.oak.spi.commit.PartialConflictHandler.Resolution r) {
        if (r == null) {
            return Resolution.IGNORED;
        }
        switch (r) {
        case OURS:
            return Resolution.OURS;
        case THEIRS:
            return Resolution.THEIRS;
        case MERGED:
            return Resolution.MERGED;
        }
        return Resolution.IGNORED;
    }

    @SuppressWarnings("deprecation")
    private static class ThreeWayConflictHandlerWrapper implements ThreeWayConflictHandler {
        private final PartialConflictHandler handler;

        public ThreeWayConflictHandlerWrapper(PartialConflictHandler handler) {
            this.handler = handler;
        }

        @Override
        public Resolution addExistingProperty(NodeBuilder parent, PropertyState ours, PropertyState theirs) {
            return wrap(handler.addExistingProperty(parent, ours, theirs));
        }

        @Override
        public Resolution changeDeletedProperty(NodeBuilder parent, PropertyState ours, PropertyState base) {
            return wrap(handler.changeDeletedProperty(parent, ours));
        }

        @Override
        public Resolution changeChangedProperty(NodeBuilder parent, PropertyState ours, PropertyState theirs,
                PropertyState base) {
            return wrap(handler.changeChangedProperty(parent, ours, theirs));
        }

        @Override
        public Resolution deleteDeletedProperty(NodeBuilder parent, PropertyState base) {
            return wrap(handler.deleteDeletedProperty(parent, base));
        }

        @Override
        public Resolution deleteChangedProperty(NodeBuilder parent, PropertyState theirs, PropertyState base) {
            return wrap(handler.deleteChangedProperty(parent, theirs));
        }

        @Override
        public Resolution addExistingNode(NodeBuilder parent, String name, NodeState ours, NodeState theirs) {
            return wrap(handler.addExistingNode(parent, name, ours, theirs));
        }

        @Override
        public Resolution changeDeletedNode(NodeBuilder parent, String name, NodeState ours, NodeState base) {
            return wrap(handler.changeDeletedNode(parent, name, ours));
        }

        @Override
        public Resolution deleteChangedNode(NodeBuilder parent, String name, NodeState theirs, NodeState base) {
            return wrap(handler.deleteChangedNode(parent, name, theirs));
        }

        @Override
        public Resolution deleteDeletedNode(NodeBuilder parent, String name, NodeState base) {
            return wrap(handler.deleteDeletedNode(parent, name));
        }
    }
}
