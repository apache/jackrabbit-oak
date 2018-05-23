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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.mongodb.event.ServerHeartbeatFailedEvent;
import com.mongodb.event.ServerHeartbeatStartedEvent;
import com.mongodb.event.ServerHeartbeatSucceededEvent;
import com.mongodb.event.ServerMonitorListener;

/**
 * A composite {@link ServerMonitorListener}.
 */
class CompositeServerMonitorListener implements ServerMonitorListener {

    private final List<ServerMonitorListener> listeners = new CopyOnWriteArrayList<>();

    void addListener(ServerMonitorListener listener) {
        listeners.add(listener);
    }

    void removeListener(ServerMonitorListener listener) {
        listeners.remove(listener);
    }

    @Override
    public void serverHearbeatStarted(ServerHeartbeatStartedEvent event) {
        listeners.forEach(l -> l.serverHearbeatStarted(event));
    }

    @Override
    public void serverHeartbeatSucceeded(ServerHeartbeatSucceededEvent event) {
        listeners.forEach(l -> l.serverHeartbeatSucceeded(event));
    }

    @Override
    public void serverHeartbeatFailed(ServerHeartbeatFailedEvent event) {
        listeners.forEach(l -> l.serverHeartbeatFailed(event));
    }
}
