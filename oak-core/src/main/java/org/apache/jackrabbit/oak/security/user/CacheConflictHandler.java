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
package org.apache.jackrabbit.oak.security.user;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.commit.DefaultThreeWayConflictHandler;
import org.apache.jackrabbit.oak.plugins.memory.PropertyBuilder;
import org.apache.jackrabbit.oak.spi.security.user.cache.CacheConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code CacheConflictHandler} takes care of merging the {@code rep:expiration} property
 * during parallel updates.
 *<p>
 * The conflict handler deals with the following conflicts:
 * <ul>
 *     <li>{@code addExistingProperty}  : {@code Resolution.IGNORED}, We should not have add conflints, since the {@code rep:{@code rep:expiration}} node is created with the user</li>
 *     <li>{@code changeDeletedProperty}: {@code Resolution.IGNORED},</li>
 *     <li>{@code changeChangedProperty}: {@code Resolution.MERGED}, the properties with higher {@code rep:expiration} get merged</li>
 *     <li>{@code deleteChangedProperty}: {@code Resolution.IGNORED} .</li>
 *     <li>{@code deleteDeletedProperty}: {@code Resolution.IGNORED}.</li>
 *     <li>{@code changeDeletedNode}    : {@code Resolution.IGNORED}, .</li>
 *     <li>{@code deleteChangedNode}    : {@code Resolution.IGNORED}, </li>
 *     <li>{@code deleteDeletedNode}    : {@code Resolution.IGNORED}.</li>
 * </ul>
 */

class CacheConflictHandler extends DefaultThreeWayConflictHandler {

    private static final Logger LOG = LoggerFactory.getLogger(CacheConflictHandler.class);

    protected CacheConflictHandler() {
        super(Resolution.IGNORED);

    }

    private Resolution resolveRepExpirationConflict(@NotNull NodeBuilder parent, @NotNull PropertyState ours, @NotNull PropertyState theirs,
                                         PropertyState base) {
        if (CacheConstants.REP_EXPIRATION.equals(ours.getName()) && CacheConstants.REP_EXPIRATION.equals(theirs.getName())){

            PropertyBuilder<Long> merged = PropertyBuilder.scalar(Type.LONG);
            merged.setName(CacheConstants.REP_EXPIRATION);

            //if base is bigger than ours and theirs, then use base. This should never happens
            if (base.getValue(Type.LONG) > ours.getValue(Type.LONG)  &&
                    base.getValue(Type.LONG) > theirs.getValue(Type.LONG)){
                merged.setValue(base.getValue(Type.LONG));
                LOG.warn("base is bigger than ours and theirs. This was supposed to never happens");
                return Resolution.MERGED;
            }

            //if ours is bigger than theirs, then use ours
            //otherwise use theirs
            if (ours.getValue(Type.LONG) > theirs.getValue(Type.LONG)){
                merged.setValue(ours.getValue(Type.LONG));
            } else {
                merged.setValue(theirs.getValue(Type.LONG));
            }
            parent.setProperty(merged.getPropertyState());
            LOG.debug("Resolved conflict for property {} our value: {}, their value {}, merged value: {}", CacheConstants.REP_EXPIRATION, ours.getValue(Type.LONG), theirs.getValue(Type.LONG), merged.getValue(0));
            return Resolution.MERGED;
        }
        return Resolution.IGNORED;

    }

    @Override
    @NotNull
    public Resolution changeChangedProperty(@NotNull NodeBuilder parent, @NotNull PropertyState ours, @NotNull PropertyState theirs,
                                            @NotNull PropertyState base) {

        return resolveRepExpirationConflict(parent, ours, theirs, base);
    }


}
