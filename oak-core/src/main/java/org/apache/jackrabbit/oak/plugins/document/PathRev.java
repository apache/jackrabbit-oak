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
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A cache key implementation, which is a combination of a path string and a
 * revision.
 */
public final class PathRev implements CacheValue {

    private static final Logger LOG = LoggerFactory.getLogger(PathRev.class);

    private final String path;

    private final RevisionVector revision;

    public PathRev(@Nonnull String path, @Nonnull RevisionVector revision) {
        this.path = checkNotNull(path);
        this.revision = checkNotNull(revision);
    }

    public String getPath() {
        return path;
    }

    @Override
    public int getMemory() {
        long size =  24                                               // shallow size
                       + (long)StringUtils.estimateMemoryUsage(path)  // path
                       + revision.getMemory();                        // revision
        if (size > Integer.MAX_VALUE) {
            LOG.debug("Estimated memory footprint larger than Integer.MAX_VALUE: {}.", size);
            size = Integer.MAX_VALUE;
        }
        return (int) size;
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
        int dim = revision.getDimensions();
        StringBuilder sb = new StringBuilder(path.length() + (Revision.REV_STRING_APPROX_SIZE + 1) * dim);
        sb.append(path);
        sb.append("@");
        revision.toStringBuilder(sb);
        return sb.toString();
    }

    public String asString() {
        return toString();
    }

    public static PathRev fromString(String s) {
        int index = s.lastIndexOf('@');
        return new PathRev(s.substring(0, index),
                RevisionVector.fromString(s.substring(index + 1)));
    }

    public int compareTo(PathRev b) {
        if (this == b) {
            return 0;
        }
        int compare = path.compareTo(b.path);
        if (compare == 0) {
            compare = revision.compareTo(b.revision);
        }
        return compare;
    }
    
}
