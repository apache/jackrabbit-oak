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
package org.apache.jackrabbit.oak.plugins.observation;

import static java.util.Collections.emptyMap;

import java.util.Map;

import org.apache.jackrabbit.api.observation.JackrabbitEvent;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;

import com.google.common.base.Objects;

/**
 * TODO document
 */
final class EventImpl implements JackrabbitEvent {

    private final EventContext context;
    private final int type;
    private final ImmutableTree tree;
    private final String name;
    private final Map<?, ?> info;

    private EventImpl(
            EventContext context, int type,
            ImmutableTree tree, String name, Map<?, ?> info) {
        this.context = context;
        this.type = type;
        this.tree = tree;
        this.name = name;
        this.info = info;
    }

    EventImpl(EventContext context, int type, ImmutableTree tree, String name) {
        this(context, type, tree, name, emptyMap());
    }

    EventImpl(EventContext context, int type, ImmutableTree tree, Map<?, ?> info) {
        this(context, type, tree, null, info);
    }

    EventImpl(EventContext context, int type, ImmutableTree tree) {
        this(context, type, tree, null, emptyMap());
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public String getPath() {
        String path = tree.getPath();
        if (name != null) {
            path = PathUtils.concat(path, name);
        }
        return context.getJcrPath(path);
    }

    @Override
    public String getUserID() {
        return context.getUserID();
    }

    @Override
    public String getIdentifier() {
        return IdentifierManager.getIdentifier(tree);
    }

    @Override
    public Map<?, ?> getInfo() {
        return info;
    }

    @Override
    public String getUserData() {
        return context.getUserData();
    }

    @Override
    public long getDate() {
        return context.getDate();
    }

    @Override
    public boolean isExternal() {
        return context.isExternal();
    }

    //------------------------------------------------------------< Object >--

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        } else if (object instanceof EventImpl) {
            EventImpl that = (EventImpl) object;
            return this.type == that.type
                    && context.equals(that.context)
                    && Objects.equal(this.info, that.info)
                    && getPath().equals(that.getPath())
                    && getIdentifier().equals(that.getIdentifier());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(context, type, info, getPath(), getIdentifier());
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).omitNullValues()
                .add("type", type)
                .add("tree", tree)
                .add("name", name)
                .add("info", info)
                .add("context", context)
                .toString();
    }

}
