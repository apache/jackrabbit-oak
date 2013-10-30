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
package org.apache.jackrabbit.oak.plugins.mongomk;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Implementation of a {@link NodeStateDiff}, which translates the diffs into
 * {@link UpdateOp}s of a commit.
 */
class CommitDiff implements NodeStateDiff {

    private final Commit commit;

    CommitDiff(@Nonnull Commit commit) {
        this.commit = checkNotNull(commit);
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        // TODO: implement
        return false;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        // TODO: implement
        return false;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        // TODO: implement
        return false;
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        // TODO: implement
        return false;
    }

    @Override
    public boolean childNodeChanged(String name,
                                    NodeState before,
                                    NodeState after) {
        // TODO: implement
        return false;
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        // TODO: implement
        return false;
    }
}
