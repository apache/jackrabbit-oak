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

import java.util.Map;

import org.apache.jackrabbit.api.observation.JackrabbitEvent;

import com.google.common.base.Objects;

/**
 * TODO document
 */
final class EventImpl implements JackrabbitEvent {

    private final EventContext context;
    private final int type;
    private final String path;
    private final String identifier;
    private final Map<?, ?> info;

    EventImpl(EventContext context,
            int type, String path, String identifier, Map<?, ?> info) {
        this.context = context;
        this.type = type;
        this.path = path;
        this.identifier = identifier;
        this.info = info;
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public String getPath() {
        return context.getJcrPath(path);
    }

    @Override
    public String getUserID() {
        return context.getUserID();
    }

    @Override
    public String getIdentifier() {
        return identifier;
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
            return Objects.equal(this.context, that.context)
                    && this.type == that.type
                    && Objects.equal(this.path, that.path)
                    && Objects.equal(this.identifier, that.identifier)
                    && Objects.equal(this.info, that.info);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(context, type, path, identifier, info);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("type", type)
                .add("path", path)
                .add("identifier", identifier)
                .add("info", info)
                .add("context", context)
                .toString();
    }

}
