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

import java.util.Comparator;

import javax.annotation.Nonnull;

/**
 * A revision context for tests.
 */
public class DummyRevisionContext implements RevisionContext {

    static final RevisionContext INSTANCE = new DummyRevisionContext();

    private final Comparator<Revision> comparator
            = StableRevisionComparator.INSTANCE;

    @Override
    public UnmergedBranches getBranches() {
        return new UnmergedBranches(comparator);
    }

    @Override
    public UnsavedModifications getPendingModifications() {
        return new UnsavedModifications();
    }

    @Override
    public Comparator<Revision> getRevisionComparator() {
        return comparator;
    }

    @Override
    public int getClusterId() {
        return 1;
    }

    @Nonnull
    @Override
    public Revision getHeadRevision() {
        return Revision.newRevision(1);
    }
}
