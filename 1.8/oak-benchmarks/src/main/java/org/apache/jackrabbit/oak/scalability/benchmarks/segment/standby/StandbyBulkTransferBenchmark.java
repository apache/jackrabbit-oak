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

package org.apache.jackrabbit.oak.scalability.benchmarks.segment.standby;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Stopwatch;
import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.jackrabbit.oak.scalability.benchmarks.ScalabilityBenchmark;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityAbstractSuite;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityAbstractSuite.ExecutionContext;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.standby.client.StandbyClientSync;
import org.apache.jackrabbit.oak.segment.standby.jmx.StandbyStatusMBean;
import org.apache.jackrabbit.oak.segment.standby.server.StandbyServerSync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bulk transfers <code>nodeCount</code> nodes from primary to standby and
 * outputs statistics related to duration, number of segments transferred and
 * number of segment bytes transferred.
 */
public class StandbyBulkTransferBenchmark extends ScalabilityBenchmark {
    protected static final Logger LOG = LoggerFactory.getLogger(ScalabilityAbstractSuite.class);

    @Override
    public void execute(Repository repository, Credentials credentials, ExecutionContext context) throws Exception {
        Map<Object, Object> contextMap = context.getMap();
        StandbyClientSync[] clientSyncs = (StandbyClientSync[]) contextMap.get("clientSyncs");
        StandbyServerSync[] serverSyncs = (StandbyServerSync[]) contextMap.get("serverSyncs");
        FileStore[] stores = (FileStore[]) contextMap.get("stores");

        stores[0].flush();
        serverSyncs[0].start();

        MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName status = new ObjectName(StandbyStatusMBean.JMX_NAME + ",id=*");
        ObjectName clientStatus = new ObjectName(clientSyncs[0].getMBeanName());
        ObjectName serverStatus = new ObjectName(serverSyncs[0].getMBeanName());

        Stopwatch stopwatch = Stopwatch.createStarted();
        clientSyncs[0].run();
        stopwatch.stop();

        Set<ObjectName> instances = jmxServer.queryNames(status, null);
        ObjectName connectionStatus = null;
        for (ObjectName s : instances) {
            if (!s.equals(clientStatus) && !s.equals(serverStatus)) {
                connectionStatus = s;
            }
        }
        assert (connectionStatus != null);

        long segments = (Long) jmxServer.getAttribute(connectionStatus, "TransferredSegments");
        long bytes = (Long) jmxServer.getAttribute(connectionStatus, "TransferredSegmentBytes");

        LOG.info("Bulk transfer for {} nodes finished! Segments = {}, segments size = {} bytes, time = {}",
                Integer.getInteger("nodeCount", 100_000), segments, bytes, stopwatch);
    }
}
