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

import org.apache.jackrabbit.oak.stats.Clock;

/**
 * Wraps an existing revision context and exposes a custom {@code clusterId}.
 */
public final class RevisionContextWrapper implements RevisionContext {

    private final RevisionContext context;
    private final int clusterId;

    public RevisionContextWrapper(RevisionContext context,
                                  int clusterId) {
        this.context = context;
        this.clusterId = clusterId;
    }

    @Override
    public UnmergedBranches getBranches() {
        return context.getBranches();
    }

    @Override
    public UnsavedModifications getPendingModifications() {
        return context.getPendingModifications();
    }

    @Override
    public int getClusterId() {
        return clusterId;
    }

    @Nonnull
    @Override
    public RevisionVector getHeadRevision() {
        return context.getHeadRevision();
    }

    @Nonnull
    @Override
    public Revision newRevision() {
        Revision r = context.newRevision();
        return new Revision(r.getTimestamp(), r.getCounter(), clusterId);
    }

    @Nonnull
    @Override
    public Clock getClock() {
        return context.getClock();
    }

    @Override
    public String getCommitValue(@Nonnull Revision revision,
                                 @Nonnull NodeDocument nodeDocument) {
        return context.getCommitValue(revision, nodeDocument);
    }
}
