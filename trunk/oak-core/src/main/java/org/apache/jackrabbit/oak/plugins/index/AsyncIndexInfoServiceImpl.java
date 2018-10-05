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

package org.apache.jackrabbit.oak.plugins.index;

import java.util.Calendar;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.util.ISO8601;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.component.annotations.ReferencePolicyOption;

@Component
public class AsyncIndexInfoServiceImpl implements AsyncIndexInfoService {

    private final Map<String, IndexStatsMBean> statsMBeans = new ConcurrentHashMap<>();

    @Reference
    private NodeStore nodeStore;

    public AsyncIndexInfoServiceImpl() {

    }

    public AsyncIndexInfoServiceImpl(NodeStore nodeStore) {
        this.nodeStore = nodeStore;
    }

    @Override
    public Iterable<String> getAsyncLanes() {
        return getAsyncLanes(nodeStore.getRoot());
    }

    @Override
    public Iterable<String> getAsyncLanes(NodeState root) {
        NodeState async = getAsyncState(root);
        Set<String> names = new HashSet<>();
        for (PropertyState ps : async.getProperties()) {
            String name = ps.getName();
            if (AsyncIndexUpdate.isAsyncLaneName(name)) {
                names.add(name);
            }
        }
        return names;
    }

    @Override
    public AsyncIndexInfo getInfo(String name) {
        return getInfo(name, nodeStore.getRoot());
    }

    @Override
    public AsyncIndexInfo getInfo(String name, NodeState root) {
        NodeState async = getAsyncState(root);
        if (async.hasProperty(name)) {
            long lastIndexedTo = getLastIndexedTo(name, async);
            long leaseEnd = -1;
            boolean running = false;
            if (async.hasProperty(AsyncIndexUpdate.leasify(name))) {
                running = true;
                leaseEnd = async.getLong(AsyncIndexUpdate.leasify(name));
            }
            IndexStatsMBean mbean = statsMBeans.get(name);
            return new AsyncIndexInfo(name, lastIndexedTo, leaseEnd, running, mbean);
        }
        return null;
    }

    @Override
    public Map<String, Long> getIndexedUptoPerLane() {
        return getIndexedUptoPerLane(nodeStore.getRoot());
    }

    @Override
    public Map<String, Long> getIndexedUptoPerLane(NodeState root) {
        ImmutableMap.Builder<String, Long> builder = new ImmutableMap.Builder<String, Long>();
        NodeState async = getAsyncState(root);
        for (PropertyState ps : async.getProperties()) {
            String name = ps.getName();
            if (AsyncIndexUpdate.isAsyncLaneName(name)) {
                long lastIndexedTo = getLastIndexedTo(name, async);
                builder.put(name, lastIndexedTo);
            }
        }
        return builder.build();
    }

    private NodeState getAsyncState(NodeState root) {
        return root.getChildNode(AsyncIndexUpdate.ASYNC);
    }

    @Reference(name = "statsMBeans",
            policy = ReferencePolicy.DYNAMIC,
            cardinality = ReferenceCardinality.MULTIPLE,
            policyOption = ReferencePolicyOption.GREEDY,
            service = IndexStatsMBean.class
    )
    protected void bindStatsMBeans(IndexStatsMBean mBean) {
        statsMBeans.put(mBean.getName(), mBean);
    }

    protected void unbindStatsMBeans(IndexStatsMBean mBean) {
        statsMBeans.remove(mBean.getName());
    }

    private static long getLastIndexedTo(String name, NodeState async) {
        return getDateAsMillis(async.getProperty(AsyncIndexUpdate.lastIndexedTo(name)));
    }

    private static long getDateAsMillis(PropertyState ps) {
        if (ps == null) {
            return -1;
        }
        String date = ps.getValue(Type.DATE);
        Calendar cal = ISO8601.parse(date);
        return cal.getTimeInMillis();
    }
}
