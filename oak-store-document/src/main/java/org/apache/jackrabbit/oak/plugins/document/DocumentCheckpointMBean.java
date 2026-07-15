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

package org.apache.jackrabbit.oak.plugins.document;

import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularDataSupport;

import org.apache.jackrabbit.oak.commons.jmx.AbstractCheckpointMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.document.Checkpoints.Info;

/**
 * {@code CheckpointMBean} implementation for the {@code DocumentNodeStore}.
 */
public class DocumentCheckpointMBean extends AbstractCheckpointMBean {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final DocumentNodeStore store;

    public DocumentCheckpointMBean(DocumentNodeStore store) {
        this.store = store;
    }

    @Override
    protected void collectCheckpoints(TabularDataSupport tab) throws OpenDataException {
        Map<Revision, Info> checkpoints = store.getCheckpoints().getCheckpoints();

        for (Entry<Revision, Info> checkpoint : checkpoints.entrySet()) {
            String id = checkpoint.getKey().toString();
            Info info = checkpoint.getValue();
            Date created = new Date(checkpoint.getKey().getTimestamp());
            Date expires = new Date(info.getExpiryTime());
            tab.put(id, toCompositeData(
                    id, created.toString(), expires.toString(), info.get()));
        }
    }

    @Override
    public long getOldestCheckpointCreationTimestamp() {
        Map<Revision, Info> checkpoints = store.getCheckpoints().getCheckpoints();

        long minTimestamp = Long.MAX_VALUE;
        for (Entry<Revision, Info> checkpoint : checkpoints.entrySet()) {
            minTimestamp = Math.min(minTimestamp, checkpoint.getKey().getTimestamp());
        }

        return (minTimestamp==Long.MAX_VALUE)?0:minTimestamp;
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
