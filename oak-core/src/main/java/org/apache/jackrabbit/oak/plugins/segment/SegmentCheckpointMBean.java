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

package org.apache.jackrabbit.oak.plugins.segment;

import java.util.Date;

import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * {@code CheckpointMBean} implementation for the {@code SegmentNodeStore}.
 */
public class SegmentCheckpointMBean implements CheckpointMBean {
    private static final String[] FIELD_NAMES = new String[] { "id", "created", "expires"};
    private static final String[] FIELD_DESCRIPTIONS = FIELD_NAMES;

    @SuppressWarnings("rawtypes")
    private static final OpenType[] FIELD_TYPES = new OpenType[] {
            SimpleType.STRING, SimpleType.STRING, SimpleType.STRING };

    private static final CompositeType TYPE = createCompositeType();

    private static CompositeType createCompositeType() {
        try {
            return new CompositeType(SegmentCheckpointMBean.class.getName(),
                    "Checkpoints", FIELD_NAMES, FIELD_DESCRIPTIONS, FIELD_TYPES);
        } catch (OpenDataException e) {
            throw new IllegalStateException(e);
        }
    }

    private final SegmentNodeStore store;

    public SegmentCheckpointMBean(SegmentNodeStore store) {
        this.store = store;
    }

    @Override
    public TabularData listCheckpoints() {
        NodeState checkpoints = store.getCheckpoints();

        try {
            TabularDataSupport tab = new TabularDataSupport(
                    new TabularType(SegmentCheckpointMBean.class.getName(),
                            "Checkpoints", TYPE, new String[] { "id" }));

            for (ChildNodeEntry cne : checkpoints.getChildNodeEntries()) {
                String id = cne.getName();
                NodeState checkpoint = cne.getNodeState();
                String created = getDate(checkpoint, "created");
                String expires = getDate(checkpoint, "timestamp");
                tab.put(id, toCompositeData(id, created, expires));
            }

            return tab;
        } catch (OpenDataException e) {
            throw new IllegalStateException(e);
        }
    }

    private static CompositeDataSupport toCompositeData(String id, String created, String expires)
            throws OpenDataException {
        return new CompositeDataSupport(TYPE, FIELD_NAMES, new String[] { id, created, expires });
    }

    private static String getDate(NodeState checkpoint, String name) {
        PropertyState p = checkpoint.getProperty(name);
        if (p == null) {
            return "NA";
        }

        return new Date(p.getValue(Type.LONG)).toString();
    }

    @Override
    public String createCheckpoint(long lifetime) {
        return store.checkpoint(lifetime);
    }

    @Override
    public boolean releaseCheckpoint(String checkpoint) {
        return store.release(checkpoint);
    }

}
