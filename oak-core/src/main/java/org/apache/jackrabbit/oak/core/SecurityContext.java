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
package org.apache.jackrabbit.oak.core;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.ReadStatus;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The security context encapsulates all the information needed to make
 * read permission checks within a specific subtree.
 */
class SecurityContext {

    /**
     * Underlying root state, used to optimize a common case
     * in {@link #equals(Object)}.
     */
    private final NodeState root;

    /**
     * Immutable tree based on the underlying node state.
     */
    private final ImmutableTree base;

    private final PermissionProvider permissionProvider;

    private ReadStatus readStatus;

    SecurityContext(
            @Nonnull NodeState rootState,
            @Nonnull PermissionProvider permissionProvider,
            @Nonnull TreeTypeProvider typeProvider) {
        this.root = checkNotNull(rootState);
        this.base = new ImmutableTree(rootState, typeProvider);
        this.permissionProvider = permissionProvider;
        // calculate the readstatus for the root
        this.readStatus = permissionProvider.getReadStatus(base, null);
    }

    private SecurityContext(
            @Nonnull SecurityContext parent,
            @Nonnull String name, @Nonnull NodeState nodeState) {
        this.root = checkNotNull(parent).root;
        this.base = new ImmutableTree(parent.base, name, nodeState);
        this.permissionProvider = parent.permissionProvider;
        if (base.getType() == parent.base.getType()) {
            readStatus = ReadStatus.getChildStatus(parent.readStatus);
        } else {
            readStatus = null;
        }
    }

    private synchronized ReadStatus getReadStatus() {
        if (readStatus == null) {
            readStatus = permissionProvider.getReadStatus(base, null);
        }
        return readStatus;
    }

    boolean canReadThisNode() {
        return getReadStatus().includes(ReadStatus.ALLOW_THIS);
    }

    boolean canReadAllProperties() {
        ReadStatus rs = getReadStatus();
        return rs.includes(ReadStatus.ALLOW_PROPERTIES);
    }

    boolean canReadProperty(PropertyState property) {
        ReadStatus rs = getReadStatus();
        if (rs.includes(ReadStatus.ALLOW_PROPERTIES)) {
            return true;
        } else if (rs.appliesToThis()) {
            rs = permissionProvider.getReadStatus(base, property);
            return rs.isAllow();
        } else {
            return false;
        }
    }

    boolean canNotReadChildNodes() {
        ReadStatus rs = getReadStatus();
        return rs.includes(ReadStatus.DENY_CHILDREN);
    }

    SecurityContext getChildContext(String name, NodeState state) {
        return new SecurityContext(this, name, state);
    }

    //-------------------------------------------------------------< Object >---

    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        } else if (object instanceof SecurityContext) {
            SecurityContext that = (SecurityContext) object;
            // TODO: We should be able to do this optimization also across
            // different revisions (root states) as long as the path,
            // the subtree, and any security-related areas like the
            // permission store are equal for both states.
            return root.equals(that.root)
                    && base.getPath().equals(that.base.getPath());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 0;
    }

}
