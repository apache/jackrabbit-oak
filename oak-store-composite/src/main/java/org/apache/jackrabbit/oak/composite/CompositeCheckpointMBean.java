/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.composite;

import org.apache.jackrabbit.oak.commons.jmx.AbstractCheckpointMBean;

import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularDataSupport;
import java.util.Date;
import java.util.Map;
import java.util.stream.StreamSupport;

import static org.apache.jackrabbit.oak.composite.CompositeNodeStore.CHECKPOINT_METADATA;

public class CompositeCheckpointMBean extends AbstractCheckpointMBean {

    private final CompositeNodeStore store;

    public CompositeCheckpointMBean(CompositeNodeStore store) {
        this.store = store;
    }

    @Override
    public String createCheckpoint(long lifetime) {
        return store.checkpoint(lifetime);
    }

    @Override
    public boolean releaseCheckpoint(String id) {
        return store.release(id);
    }

    @Override
    protected void collectCheckpoints(TabularDataSupport tab) throws OpenDataException {
        for (String id : store.checkpoints()) {
            Map<String, String> info = store.allCheckpointInfo(id);
            tab.put(id, toCompositeData(
                    id,
                    getDate(info, CHECKPOINT_METADATA + "created"),
                    getDate(info, CHECKPOINT_METADATA + "expires"),
                    store.checkpointInfo(id)));
        }
    }

    @Override
    public long getOldestCheckpointCreationTimestamp() {
        return StreamSupport.stream(store.checkpoints().spliterator(), false)
                .map(store::allCheckpointInfo)
                .map(i -> i.get(CHECKPOINT_METADATA + "created"))
                .mapToLong(l -> l == null ? 0 : Long.valueOf(l))
                .sorted()
                .findFirst()
                .orElse(0);
    }

    private static String getDate(Map<String, String> info, String name) {
        String p = info.get(name);
        if (p == null) {
            return "NA";
        }
        try {
            return new Date(Long.valueOf(p)).toString();
        } catch (NumberFormatException e) {
            return "NA";
        }
    }
}
