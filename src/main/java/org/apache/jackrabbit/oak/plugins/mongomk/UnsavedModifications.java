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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Keeps track of when nodes where last modified. To be persisted later by
 * a background thread.
 */
class UnsavedModifications {

    private final ConcurrentHashMap<String, Revision> map = new ConcurrentHashMap<String, Revision>();

    @CheckForNull
    public Revision put(@Nonnull String path, @Nonnull Revision revision) {
        return map.put(checkNotNull(path), checkNotNull(revision));
    }

    @CheckForNull
    public Revision get(String path) {
        return map.get(path);
    }

    @CheckForNull
    public Revision remove(String path) {
        return map.remove(path);
    }

    @Nonnull
    public Collection<String> getPaths() {
        return map.keySet();
    }

    /**
     * Applies all modifications from this instance to the <code>other</code>.
     * A modification is only applied if there is no modification in other
     * for a given path or if the other modification is earlier.
     *
     * @param other the other <code>UnsavedModifications</code>.
     */
    public void applyTo(UnsavedModifications other) {
        for (Map.Entry<String, Revision> entry : map.entrySet()) {
            Revision r = other.map.putIfAbsent(entry.getKey(), entry.getValue());
            if (r != null) {
                if (r.compareRevisionTime(entry.getValue()) < 0) {
                    other.map.put(entry.getKey(), entry.getValue());
                }
            }
        }
    }
}
