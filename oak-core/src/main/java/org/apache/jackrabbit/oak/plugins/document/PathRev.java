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
package org.apache.jackrabbit.oak.plugins.document;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.cache.CacheValue;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A cache key implementation, which is a combination of a path string and a
 * revision.
 */
final public class PathRev implements CacheValue {

    private final String path;

    private final Revision revision;

    public PathRev(@Nonnull String path, @Nonnull Revision revision) {
        this.path = checkNotNull(path);
        this.revision = checkNotNull(revision);
    }

    @Override
    public int getMemory() {
        return 24                           // shallow size
                + 40 + path.length() * 2    // path
                + 32;                       // revision
    }

    //----------------------------< Object >------------------------------------


    @Override
    public int hashCode() {
        return path.hashCode() ^ revision.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof PathRev) {
            PathRev other = (PathRev) obj;
            return revision.equals(other.revision) && path.equals(other.path);
        }
        return false;
    }

    @Override
    public String toString() {
        return path + "@" + revision;
    }
}
