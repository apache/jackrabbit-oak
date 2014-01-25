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

import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;

/**
 * Information shared by all events coming from a single commit.
 */
final class EventContext {

    /**
     * Dummy session identifier used to identify external commits.
     */
    private static final String OAK_EXTERNAL = "oak:external";

    private final NamePathMapper mapper;

    private final CommitInfo info;

    EventContext(NamePathMapper mapper, CommitInfo info) {
        this.mapper = mapper;
        if (info != null) {
            this.info = info;
        } else {
            // Generate a dummy CommitInfo object to avoid extra null checks.
            // The current time is used as a rough estimate of the commit time.
            this.info = new CommitInfo(OAK_EXTERNAL, null, null);
        }
    }

    String getJcrName(String name) {
        return mapper.getJcrName(name);
    }

    String getJcrPath(String path) {
        return mapper.getJcrPath(path);
    }

    String getUserID() {
        return info.getUserId();
    }

    String getUserData() {
        return info.getMessage();
    }

    long getDate() {
        return info.getDate();
    }

    boolean isExternal() {
        return info.getSessionId() == OAK_EXTERNAL;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        } else if (object instanceof EventContext) {
            EventContext that = (EventContext) object;
            return info.equals(that.info);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return info.hashCode();
    }

    @Override
    public String toString() {
        return info.toString();
    }

}