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
 * A revision context for tests.
 */
public class DummyRevisionContext implements RevisionContext {

    static final RevisionContext INSTANCE = new DummyRevisionContext();

    @Override
    public UnmergedBranches getBranches() {
        return new UnmergedBranches();
    }

    @Override
    public UnsavedModifications getPendingModifications() {
        return new UnsavedModifications();
    }

    @Override
    public int getClusterId() {
        return 1;
    }

    @Nonnull
    @Override
    public RevisionVector getHeadRevision() {
        return new RevisionVector(Revision.newRevision(getClusterId()));
    }

    @Nonnull
    @Override
    public Revision newRevision() {
        return Revision.newRevision(getClusterId());
    }

    @Nonnull
    @Override
    public Clock getClock() {
        return Clock.SIMPLE;
    }

    @Override
    public String getCommitValue(@Nonnull Revision changeRevision,
                                 @Nonnull NodeDocument doc) {
        return doc.resolveCommitValue(changeRevision);
    }
}
