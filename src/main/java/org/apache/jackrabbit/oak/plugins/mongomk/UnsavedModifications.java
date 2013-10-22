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

    /**
     * Puts a revision for the given path. The revision for the given path is
     * only put if there is no modification present for the revision or if the
     * current modification revision is older than the passed revision.
     *
     * @param path the path of the modified node.
     * @param revision the revision of the modification.
     * @return the previously set revision for the given path or null if there
     *          was none or the current revision is newer.
     */
    @CheckForNull
    public Revision put(@Nonnull String path, @Nonnull Revision revision) {
        checkNotNull(path);
        checkNotNull(revision);
        for (;;) {
            Revision previous = map.get(path);
            if (previous == null) {
                if (map.putIfAbsent(path, revision) == null) {
                    return null;
                }
            } else {
                if (previous.compareRevisionTime(revision) < 0) {
                    if (map.replace(path, previous, revision)) {
                        return previous;
                    }
                } else {
                    // revision is earlier, do not update
                    return null;
                }
            }
        }
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
     * for a given path or if the other modification is earlier than the
     * merge commit revision.
     *
     * @param other the other <code>UnsavedModifications</code>.
     * @param mergeCommit the merge commit revision.
     */
    public void applyTo(UnsavedModifications other, Revision mergeCommit) {
        for (Map.Entry<String, Revision> entry : map.entrySet()) {
            other.put(entry.getKey(), mergeCommit);
        }
    }
}
