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
package org.apache.jackrabbit.oak.plugins.document;

import java.lang.management.ManagementFactory;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.document.Document.ID;

/**
 * Information about a cluster node.
 */
public class ClusterNodeInfo {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterNodeInfo.class);

    /**
     * The prefix for random (non-reusable) keys.
     */
    private static final String RANDOM_PREFIX = "random:";
    
    /**
     * The machine id.
     */
    private static final String MACHINE_ID_KEY = "machine";
    
    /**
     * The unique instance id within this machine (the current working directory
     * if not set).
     */
    private static final String INSTANCE_ID_KEY = "instance";

    /**
     * The end of the lease.
     */
    private static final String LEASE_END_KEY = "leaseEnd";

    /**
     * Additional info, such as the process id, for support.
     */
    private static final String INFO_KEY = "info";
    
    /**
     * The read/write mode key.
     */
    private static final String READ_WRITE_MODE_KEY = "readWriteMode";

    /**
     * The unique machine id (the MAC address if available).
     */
    private static final String MACHINE_ID = getMachineId();

    /**
     * The process id (if available).
     */
    private static final long PROCESS_ID = getProcessId();
    
    /**
     * The current working directory.
     */
    private static final String WORKING_DIR = System.getProperty("user.dir", "");
    
    /**
     * The number of milliseconds for a lease (1 minute by default, and
     * initially).
     */
    private long leaseTime = 1000 * 60;
    
    /**
     * The assigned cluster id.
     */
    private final int id;
    
    /**
     * The machine id.
     */
    private final String machineId;
    
    /**
     * The instance id.
     */
    private final String instanceId;
    
    /**
     * The document store that is used to renew the lease.
     */
    private final DocumentStore store;
    
    /**
     * The time (in milliseconds UTC) where this instance was started.
     */
    private final long startTime;

    /**
     * A unique id.
     */
    private final String uuid = UUID.randomUUID().toString();
    
    /**
     * The time (in milliseconds UTC) where the lease of this instance ends.
     */
    private long leaseEndTime;
    
    /**
     * The read/write mode.
     */
    private String readWriteMode;
    
    ClusterNodeInfo(int id, DocumentStore store, String machineId, String instanceId) {
        this.id = id;
        this.startTime = System.currentTimeMillis();
        this.leaseEndTime = startTime;
        this.store = store;
        this.machineId = machineId;
        this.instanceId = instanceId;
    }
    
    public int getId() {
        return id;
    }
    
    /**
     * Create a cluster node info instance for the store, with the 
     * 
     * @param store the document store (for the lease)
     * @return the cluster node info
     */
    public static ClusterNodeInfo getInstance(DocumentStore store) {
        return getInstance(store, MACHINE_ID, WORKING_DIR);
    }

    /**
     * Create a cluster node info instance for the store.
     * 
     * @param store the document store (for the lease)
     * @param machineId the machine id (null for MAC address)
     * @param instanceId the instance id (null for current working directory)
     * @return the cluster node info
     */
    public static ClusterNodeInfo getInstance(DocumentStore store, String machineId, String instanceId) {
        if (machineId == null) {
            machineId = MACHINE_ID;
        }
        if (instanceId == null) {
            instanceId = WORKING_DIR;
        }
        for (int i = 0; i < 10; i++) {
            ClusterNodeInfo clusterNode = createInstance(store, machineId, instanceId);
            UpdateOp update = new UpdateOp("" + clusterNode.id, true);
            update.set(ID, String.valueOf(clusterNode.id));
            update.set(MACHINE_ID_KEY, clusterNode.machineId);
            update.set(INSTANCE_ID_KEY, clusterNode.instanceId);
            update.set(LEASE_END_KEY, System.currentTimeMillis() + clusterNode.leaseTime);
            update.set(INFO_KEY, clusterNode.toString());
            boolean success = store.create(Collection.CLUSTER_NODES, Collections.singletonList(update));
            if (success) {
                return clusterNode;
            }
        }
        throw new MicroKernelException("Could not get cluster node info");
    }
    
    private static ClusterNodeInfo createInstance(DocumentStore store, String machineId, String instanceId) {
        long now = System.currentTimeMillis();
        // keys between "0" and "a" includes all possible numbers
        List<ClusterNodeInfoDocument> list = store.query(Collection.CLUSTER_NODES,
                "0", "a", Integer.MAX_VALUE);
        int clusterNodeId = 0;
        int maxId = 0;
        for (Document doc : list) {
            String key = doc.getId();
            int id;
            try {
                id = Integer.parseInt(key);
            } catch (Exception e) {
                // not an integer - ignore
                continue;
            }
            maxId = Math.max(maxId, id);
            Long leaseEnd = (Long) doc.get(LEASE_END_KEY);
            if (leaseEnd != null && leaseEnd > now) {
                continue;
            }
            String mId = "" + doc.get(MACHINE_ID_KEY);
            String iId = "" + doc.get(INSTANCE_ID_KEY);
            if (machineId.startsWith(RANDOM_PREFIX)) {
                // remove expired entries with random keys
                store.remove(Collection.CLUSTER_NODES, key);
                continue;
            }
            if (!mId.equals(machineId) || 
                    !iId.equals(instanceId)) {
                // a different machine or instance
                continue;
            }
            // remove expired matching entries
            store.remove(Collection.CLUSTER_NODES, key);
            if (clusterNodeId == 0 || id < clusterNodeId) {
                // if there are multiple, use the smallest value
                clusterNodeId = id;
            }
        }
        if (clusterNodeId == 0) {
            clusterNodeId = maxId + 1;
        }
        return new ClusterNodeInfo(clusterNodeId, store, machineId, instanceId);
    }
    
    /**
     * Renew the cluster id lease. This method needs to be called once in a while,
     * to ensure the same cluster id is not re-used by a different instance.
     * 
     * @param nextCheckMillis the millisecond offset
     */
    public void renewLease(long nextCheckMillis) {
        long now = System.currentTimeMillis();
        if (now + nextCheckMillis + nextCheckMillis < leaseEndTime) {
            return;
        }
        UpdateOp update = new UpdateOp("" + id, true);
        leaseEndTime = now + leaseTime;
        update.set(LEASE_END_KEY, leaseEndTime);
        ClusterNodeInfoDocument doc = store.createOrUpdate(Collection.CLUSTER_NODES, update);
        String mode = (String) doc.get(READ_WRITE_MODE_KEY);
        if (mode != null && !mode.equals(readWriteMode)) {
            readWriteMode = mode;
            store.setReadWriteMode(mode);
        }
    }
    
    public void setLeaseTime(long leaseTime) {
        this.leaseTime = leaseTime;
    }
    
    public long getLeaseTime() {
        return leaseTime;
    }
    
    public void dispose() {
        UpdateOp update = new UpdateOp("" + id, true);
        update.set(LEASE_END_KEY, null);
        store.createOrUpdate(Collection.CLUSTER_NODES, update);
    }
    
    @Override
    public String toString() {
        return "id: " + id + ",\n" +
                "startTime: " + startTime + ",\n" +
                "machineId: " + machineId + ",\n" +
                "instanceId: " + instanceId + ",\n" +
                "pid: " + PROCESS_ID + ",\n" +
                "uuid: " + uuid +",\n" +
                "readWriteMode: " + readWriteMode;
    }
        
    private static long getProcessId() {
        try {
            String name = ManagementFactory.getRuntimeMXBean().getName();
            return Long.parseLong(name.substring(0, name.indexOf('@')));
        } catch (Exception e) {
            LOG.warn("Could not get process id", e);
            return 0;
        }
    }
    
    /**
     * Calculate the unique machine id. This is the lowest MAC address if
     * available. As an alternative, a randomly generated UUID is used.
     * 
     * @return the unique id
     */
    private static String getMachineId() {
        try {
            ArrayList<String> list = new ArrayList<String>();
            Enumeration<NetworkInterface> e = NetworkInterface
                    .getNetworkInterfaces();
            while (e.hasMoreElements()) {
                NetworkInterface ni = e.nextElement();
                byte[] mac = ni.getHardwareAddress();
                if (mac != null) {
                    String x = StringUtils.convertBytesToHex(mac);
                    list.add(x);
                }
            }
            if (list.size() > 0) {
                // use the lowest value, such that if the order changes,
                // the same one is used
                Collections.sort(list);
                return "mac:" + list.get(0);
            }
        } catch (Exception e) {
            LOG.error("Error calculating the machine id", e);
        }
        return RANDOM_PREFIX + UUID.randomUUID().toString();
    }

}
