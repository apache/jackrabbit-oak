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

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A cache key implementation, which is a combination of a name, path and a
 * revision vector.
 */
public final class NamePathRev implements CacheValue, Comparable<NamePathRev> {

    private static final Logger LOG = LoggerFactory.getLogger(NamePathRev.class);

    @NotNull
    private final String name;

    @NotNull
    private final Path path;

    @NotNull
    private final RevisionVector revision;

    public NamePathRev(@NotNull String name,
                       @NotNull Path path,
                       @NotNull RevisionVector revision) {
        this.name = checkNotNull(name);
        this.path = checkNotNull(path);
        this.revision = checkNotNull(revision);
    }

    @NotNull
    public Path getPath() {
        return path;
    }

    @NotNull
    public String getName() {
        return name;
    }

    @NotNull
    public RevisionVector getRevision() {
        return revision;
    }

    @Override
    public int getMemory() {
        long size = 24L // shallow size
                + path.getMemory()
                + StringUtils.estimateMemoryUsage(name)
                + revision.getMemory();
        if (size > Integer.MAX_VALUE) {
            LOG.debug("Estimated memory footprint larger than Integer.MAX_VALUE: {}.", size);
            size = Integer.MAX_VALUE;
        }
        return (int) size;
    }

    //---------------------------< Comparable >---------------------------------

    public int compareTo(@NotNull NamePathRev other) {
        if (this == other) {
            return 0;
        }
        int compare = this.name.compareTo(other.name);
        if (compare != 0) {
            return compare;
        }
        compare = this.path.compareTo(other.path);
        if (compare != 0) {
            return compare;
        }
        return this.revision.compareTo(other.revision);
    }

    //----------------------------< Object >------------------------------------

    @Override
    public int hashCode() {
        int h = 17;
        h = 37 * h + name.hashCode();
        h = 37 * h + path.hashCode();
        h = 37 * h + revision.hashCode();
        return h;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof NamePathRev) {
            NamePathRev other = (NamePathRev) obj;
            return revision.equals(other.revision)
                    && name.equals(other.name)
                    && path.equals(other.path);
        }
        return false;
    }

    @Override
    public String toString() {
        int dim = revision.getDimensions();
        int len = path.length() + name.length();
        StringBuilder sb = new StringBuilder(len + (Revision.REV_STRING_APPROX_SIZE + 1) * dim);
        sb.append(name);
        path.toStringBuilder(sb).append('@');
        revision.toStringBuilder(sb);
        return sb.toString();
    }
}
