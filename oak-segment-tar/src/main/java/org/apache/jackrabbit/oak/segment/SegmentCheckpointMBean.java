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

package org.apache.jackrabbit.oak.segment;

import java.util.Date;

import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularDataSupport;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.commons.jmx.AbstractCheckpointMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code CheckpointMBean} implementation for the {@code SegmentNodeStore}.
 */
public class SegmentCheckpointMBean extends AbstractCheckpointMBean {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final SegmentNodeStore store;

    public SegmentCheckpointMBean(SegmentNodeStore store) {
        this.store = store;
    }

    @Override
    protected void collectCheckpoints(TabularDataSupport tab) throws OpenDataException {
        for (ChildNodeEntry cne : store.getCheckpoints().getChildNodeEntries()) {
            String id = cne.getName();
            NodeState checkpoint = cne.getNodeState();
            String created = getDate(checkpoint, "created");
            String expires = getDate(checkpoint, "timestamp");
            tab.put(id, toCompositeData(id, created, expires, store.checkpointInfo(id)));
        }
    }

    @Override
    public long getOldestCheckpointCreationTimestamp() {
        long minTimestamp = Long.MAX_VALUE;
        for (ChildNodeEntry cne : store.getCheckpoints().getChildNodeEntries()) {
            NodeState checkpoint = cne.getNodeState();
            PropertyState p = checkpoint.getProperty("created");
            if (p != null) {
                minTimestamp = Math.min(minTimestamp, p.getValue(Type.LONG));
            }
        }
        return (minTimestamp==Long.MAX_VALUE)?0:minTimestamp;
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
        String cp = store.checkpoint(lifetime);
        log.info("Created checkpoint [{}] with lifetime {}", cp, lifetime);
        return cp;
    }

    @Override
    public boolean releaseCheckpoint(String checkpoint) {
        log.info("Released checkpoint [{}]", checkpoint);
        return store.release(checkpoint);
    }

}
