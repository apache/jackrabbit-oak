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

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A cache key implementation, which is a combination of a path and a revision
 * vector.
 */
public final class PathRev implements CacheValue, Comparable<PathRev> {

    private static final Logger LOG = LoggerFactory.getLogger(PathRev.class);

    private final Path path;

    private final RevisionVector revision;

    private int hash;

    public PathRev(@NotNull Path path, @NotNull RevisionVector revision) {
        this.path = checkNotNull(path);
        this.revision = checkNotNull(revision);
    }

    public Path getPath() {
        return path;
    }

    public RevisionVector getRevision() {
        return revision;
    }

    @Override
    public int getMemory() {
        long size =  24L                          // shallow size
                       + path.getMemory()         // path
                       + revision.getMemory();    // revision
        if (size > Integer.MAX_VALUE) {
            LOG.debug("Estimated memory footprint larger than Integer.MAX_VALUE: {}.", size);
            size = Integer.MAX_VALUE;
        }
        return (int) size;
    }

    //---------------------------< Comparable >---------------------------------

    public int compareTo(@NotNull PathRev other) {
        if (this == other) {
            return 0;
        }
        int compare = this.path.compareTo(other.path);
        if (compare != 0) {
            return compare;
        }
        return this.revision.compareTo(other.revision);
    }

    //----------------------------< Object >------------------------------------

    @Override
    public int hashCode() {
        int h = this.hash;
        if (h == 0) {
            h = 17;
            h = 37 * h + path.hashCode();
            h = 37 * h + revision.hashCode();
            this.hash = h;
        }
        return h;
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
        int dim = revision.getDimensions();
        StringBuilder sb = new StringBuilder(path.length() + (Revision.REV_STRING_APPROX_SIZE + 1) * dim);
        sb.append(path);
        sb.append("@");
        revision.toStringBuilder(sb);
        return sb.toString();
    }

}
