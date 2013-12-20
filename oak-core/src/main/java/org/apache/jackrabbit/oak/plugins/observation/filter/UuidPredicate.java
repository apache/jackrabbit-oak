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

package org.apache.jackrabbit.oak.plugins.observation.filter;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;

import javax.annotation.Nonnull;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A predicate for matching against a list of UUIDs. This predicate holds
 * whenever the {@code NodeState} passed to its apply functions has a {@code jcr:uuid}
 * property and the value of that property matches any of the UUIDs that
 * has been passed to the predicate's constructor.
 */
public class UuidPredicate implements Predicate<NodeState> {
    private final String[] uuids;

    /**
     * @param uuids    uuids
     */
    public UuidPredicate(@Nonnull String[] uuids) {
        this.uuids = checkNotNull(uuids);
    }

    @Override
    public boolean apply(NodeState node) {
        if (uuids.length == 0) {
            return false;
        }

        PropertyState uuidProperty = node.getProperty(JCR_UUID);
        if (uuidProperty == null) {
            return false;
        }

        String parentUuid = uuidProperty.getValue(Type.STRING);
        for (String uuid : uuids) {
            if (parentUuid.equals(uuid)) {
                return true;
            }
        }
        return false;
    }
}
