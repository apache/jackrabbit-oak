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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ConflictHandlers {

    private ConflictHandlers() {
    }

    @SuppressWarnings("deprecation")
    @NotNull
    public static ThreeWayConflictHandler wrap(@NotNull PartialConflictHandler handler) {
        return new ThreeWayConflictHandlerWrapper(handler);
    }

    @SuppressWarnings("deprecation")
    @NotNull
    private static Resolution wrap(@Nullable org.apache.jackrabbit.oak.spi.commit.PartialConflictHandler.Resolution r) {
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

        public ThreeWayConflictHandlerWrapper(@NotNull PartialConflictHandler handler) {
            this.handler = handler;
        }

        @NotNull
        @Override
        public Resolution addExistingProperty(@NotNull NodeBuilder parent, @NotNull PropertyState ours, @NotNull PropertyState theirs) {
            return wrap(handler.addExistingProperty(parent, ours, theirs));
        }

        @NotNull
        @Override
        public Resolution changeDeletedProperty(@NotNull NodeBuilder parent, @NotNull PropertyState ours, @NotNull PropertyState base) {
            return wrap(handler.changeDeletedProperty(parent, ours));
        }

        @NotNull
        @Override
        public Resolution changeChangedProperty(@NotNull NodeBuilder parent, @NotNull PropertyState ours, @NotNull PropertyState theirs,
                                                @NotNull PropertyState base) {
            return wrap(handler.changeChangedProperty(parent, ours, theirs));
        }

        @NotNull
        @Override
        public Resolution deleteDeletedProperty(@NotNull NodeBuilder parent, @NotNull PropertyState base) {
            return wrap(handler.deleteDeletedProperty(parent, base));
        }

        @NotNull
        @Override
        public Resolution deleteChangedProperty(@NotNull NodeBuilder parent, @NotNull PropertyState theirs, @NotNull PropertyState base) {
            return wrap(handler.deleteChangedProperty(parent, theirs));
        }

        @NotNull
        @Override
        public Resolution addExistingNode(@NotNull NodeBuilder parent, @NotNull String name, @NotNull NodeState ours, @NotNull NodeState theirs) {
            return wrap(handler.addExistingNode(parent, name, ours, theirs));
        }

        @NotNull
        @Override
        public Resolution changeDeletedNode(@NotNull NodeBuilder parent, @NotNull String name, @NotNull NodeState ours, @NotNull NodeState base) {
            return wrap(handler.changeDeletedNode(parent, name, ours));
        }

        @NotNull
        @Override
        public Resolution deleteChangedNode(@NotNull NodeBuilder parent, @NotNull String name, @NotNull NodeState theirs, @NotNull NodeState base) {
            return wrap(handler.deleteChangedNode(parent, name, theirs));
        }

        @NotNull
        @Override
        public Resolution deleteDeletedNode(@NotNull NodeBuilder parent, @NotNull String name, @NotNull NodeState base) {
            return wrap(handler.deleteDeletedNode(parent, name));
        }
    }
}
