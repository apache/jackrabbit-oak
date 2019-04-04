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

package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import static org.apache.jackrabbit.util.ISO8601.parse;

import java.util.Calendar;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

/**
 * Conflict handler that merges concurrent updates to external entities on the
 * {@code ExternalIdentityConstants.REP_LAST_SYNCED} property by picking the
 * older of the 2 conflicting dates.
 */
class ExternalIdentityConflictHandler implements ThreeWayConflictHandler {

    @NotNull
    @Override
    public Resolution addExistingProperty(NodeBuilder parent, PropertyState ours, PropertyState theirs) {
        if (ExternalIdentityConstants.REP_LAST_SYNCED.equals(ours.getName())) {
            merge(parent, ours, theirs);
            return Resolution.MERGED;
        }
        return Resolution.IGNORED;
    }

    @NotNull
    @Override
    public Resolution changeChangedProperty(NodeBuilder parent, PropertyState ours, PropertyState theirs,
            PropertyState base) {
        if (ExternalIdentityConstants.REP_LAST_SYNCED.equals(ours.getName())) {
            merge(parent, ours, theirs);
            return Resolution.MERGED;
        }
        return Resolution.IGNORED;
    }

    private static void merge(NodeBuilder parent, PropertyState ours, PropertyState theirs) {
        Calendar o = parse(ours.getValue(Type.DATE));
        Calendar t = parse(theirs.getValue(Type.DATE));
        Calendar v = o.before(t) ? t : o;
        parent.setProperty(ours.getName(), v);
    }

    @Override
    @NotNull
    public Resolution changeDeletedProperty(@NotNull NodeBuilder parent, @NotNull PropertyState ours,
            @NotNull PropertyState base) {
        return Resolution.IGNORED;
    }

    @Override
    @NotNull
    public Resolution deleteDeletedProperty(@NotNull NodeBuilder parent, @NotNull PropertyState base) {
        return Resolution.IGNORED;
    }

    @Override
    @NotNull
    public Resolution deleteChangedProperty(@NotNull NodeBuilder parent, @NotNull PropertyState theirs,
            @NotNull PropertyState base) {
        return Resolution.IGNORED;
    }

    @Override
    @NotNull
    public Resolution addExistingNode(@NotNull NodeBuilder parent, @NotNull String name, @NotNull NodeState ours,
            @NotNull NodeState theirs) {
        return Resolution.IGNORED;
    }

    @Override
    @NotNull
    public Resolution changeDeletedNode(@NotNull NodeBuilder parent, @NotNull String name, @NotNull NodeState ours,
            @NotNull NodeState base) {
        return Resolution.IGNORED;
    }

    @Override
    @NotNull
    public Resolution deleteChangedNode(@NotNull NodeBuilder parent, @NotNull String name, @NotNull NodeState theirs,
            @NotNull NodeState base) {
        return Resolution.IGNORED;
    }

    @Override
    @NotNull
    public Resolution deleteDeletedNode(@NotNull NodeBuilder parent, @NotNull String name, @NotNull NodeState base) {
        return Resolution.IGNORED;
    }
}
