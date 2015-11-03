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
package org.apache.jackrabbit.oak.security.authorization.permission;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Objects;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.util.Text;

final class PermissionEntry implements Comparable<PermissionEntry>, PermissionConstants {

    /**
     * flag controls if this is an allow or deny entry
     */
    final boolean isAllow;

    /**
     * the privilege bits
     */
    final PrivilegeBits privilegeBits;

    /**
     * the index (order) of the original ACE in the ACL.
     */
    private final int index;

    /**
     * the access controlled (node) path
     */
    private final String path;

    /**
     * the restriction pattern for this entry
     */
    final RestrictionPattern restriction;

    PermissionEntry(@Nonnull String path, boolean isAllow, int index,
                    @Nonnull PrivilegeBits privilegeBits,
                    @Nonnull RestrictionPattern restriction) {
        this.path = path;
        this.isAllow = isAllow;
        this.index = index;
        this.privilegeBits = privilegeBits;
        this.restriction = restriction;
    }

    boolean matches(@Nonnull Tree tree, @Nullable PropertyState property) {
        return restriction == RestrictionPattern.EMPTY || restriction.matches(tree, property);
    }

    boolean matches(@Nonnull String treePath) {
        return restriction == RestrictionPattern.EMPTY || restriction.matches(treePath);
    }

    boolean matches() {
        return restriction == RestrictionPattern.EMPTY || restriction.matches();
    }

    boolean matchesParent(@Nonnull String parentPath) {
        return Text.isDescendantOrEqual(path, parentPath) && (restriction == RestrictionPattern.EMPTY || restriction.matches(parentPath));
    }

    @Override
    public int compareTo(@Nonnull PermissionEntry pe) {
        if (Objects.equal(path, pe.path)) {
            // reverse order
            if (index == pe.index) {
                return 0;
            } else if (index < pe.index) {
                return 1;
            } else {
                return -1;
            }
        } else {
            final int depth = PathUtils.getDepth(path);
            final int otherDepth = PathUtils.getDepth(pe.path);
            if (depth == otherDepth) {
                return path.compareTo(pe.path);
            } else {
                return (depth < otherDepth) ? 1 : -1;
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof PermissionEntry) {
            PermissionEntry that = (PermissionEntry) o;

            return index == that.index && isAllow == that.isAllow
                    && privilegeBits.equals(that.privilegeBits)
                    && path.equals(that.path) && restriction.equals(that.restriction);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(privilegeBits, index, path, isAllow, restriction);
    }
}