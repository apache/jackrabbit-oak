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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventGenerator.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * TODO UuidFilter
 * TODO Clarify: filter applies to parent
 */
public class UuidFilter implements Filter {
    private final NodeState after;
    private final String[] uuids;

    public UuidFilter(@Nonnull NodeState after, @Nonnull String[] uuids) {
        this.after = checkNotNull(after);
        this.uuids = checkNotNull(uuids);
    }

    @Override
    public boolean includeAdd(PropertyState after) {
        return includeByUuid();
    }

    @Override
    public boolean includeChange(PropertyState before, PropertyState after) {
        return includeByUuid();
    }

    @Override
    public boolean includeDelete(PropertyState before) {
        return includeByUuid();
    }

    @Override
    public boolean includeAdd(String name, NodeState after) {
        return includeByUuid();
    }

    @Override
    public boolean includeChange(String name, NodeState before, NodeState after) {
        return true;
    }

    @Override
    public boolean includeDelete(String name, NodeState before) {
        return includeByUuid();
    }

    @Override
    public boolean includeMove(String sourcePath, String destPath, NodeState moved) {
        return includeByUuid();
    }

    @Override
    public Filter create(String name, NodeState before, NodeState after) {
        return new UuidFilter(after, uuids);
    }

    //------------------------------------------------------------< private >---

    private boolean includeByUuid() {
        if (uuids.length == 0) {
            return false;
        }

        PropertyState uuidProperty = after.getProperty(JcrConstants.JCR_UUID);
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
