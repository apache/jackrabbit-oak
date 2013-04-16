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

package org.apache.jackrabbit.oak.plugins.observation2;

import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.EVENTS;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.LISTENERS;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.REP_OBSERVATION;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.observation2.EventQueueWriter.EventRecorder;
import org.apache.jackrabbit.oak.plugins.observation2.EventQueueWriter.ListenerSpec;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * TODO document
 */
public class EventQueueWriterProvider implements EditorProvider {
    public static final AtomicLong BUNDLE_ID = new AtomicLong();

    @Override
    public Editor getRootEditor(NodeState before, NodeState after, NodeBuilder builder) {
        NodeBuilder queue = getEventQueue(builder);
        if (queue == null) {
            return DefaultEditor.INSTANCE;
        }

        Iterable<ListenerSpec> listenerSpecs = getListenerSpecs(after);
        if (listenerSpecs == null) {
            return DefaultEditor.INSTANCE;
        }

        EventRecorder eventRecorder = new EventRecorder(queue, listenerSpecs);
        return VisibleEditor.wrap(new EventQueueWriter(eventRecorder, "/", after));
    }

    private static NodeBuilder getEventQueue(NodeBuilder builder) {
        if (builder.hasChildNode(JCR_SYSTEM)) {
            builder = builder.child(JCR_SYSTEM);
            if (builder.hasChildNode(REP_OBSERVATION)) {
                builder = builder.child(REP_OBSERVATION);
                return builder.child(EVENTS).child(String.valueOf(BUNDLE_ID.getAndIncrement()));
            }
        }
        return null;
    }

    private static Iterable<ListenerSpec> getListenerSpecs(NodeState after) {
        NodeState listeners = after.getChildNode(JCR_SYSTEM)
                .getChildNode(REP_OBSERVATION)
                .getChildNode(LISTENERS);

        ReadOnlyNodeTypeManager ntMgr = ReadOnlyNodeTypeManager.getInstance(after);
        Set<ListenerSpec> specs = Sets.newHashSet();
        for (ChildNodeEntry listener : listeners.getChildNodeEntries()) {
            ListenerSpec spec = ListenerSpec.create(ntMgr, listener.getName(), listener.getNodeState());
            if (spec != null) {
                specs.add(spec);
            }
        }

        return specs.isEmpty() ? null : specs;
    }

}
