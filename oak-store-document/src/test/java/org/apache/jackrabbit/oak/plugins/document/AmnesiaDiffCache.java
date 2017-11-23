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

import java.util.Collections;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.cache.CacheStats;

/**
 * A diff cache implementation, which immediately forgets the diff.
 */
class AmnesiaDiffCache extends DiffCache {

    static final DiffCache INSTANCE = new AmnesiaDiffCache();

    private AmnesiaDiffCache() {
        super();
    }

    @Override
    public String getChanges(@Nonnull RevisionVector from,
                             @Nonnull RevisionVector to,
                             @Nonnull String path,
                             @Nullable Loader loader) {
        if (loader != null) {
            return loader.call();
        }
        return null;
    }

    @Nonnull
    @Override
    public Entry newEntry(@Nonnull RevisionVector from, @Nonnull RevisionVector to, boolean local) {
        return new Entry() {
            @Override
            public void append(@Nonnull String path, @Nonnull String changes) {
            }

            @Override
            public boolean done() {
                return false;
            }
        };
    }

    @Nonnull
    @Override
    public Iterable<CacheStats> getStats() {
        return Collections.emptyList();
    }
}
