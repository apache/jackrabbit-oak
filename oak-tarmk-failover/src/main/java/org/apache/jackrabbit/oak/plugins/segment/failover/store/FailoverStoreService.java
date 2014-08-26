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
package org.apache.jackrabbit.oak.plugins.segment.failover.store;

import static java.lang.String.valueOf;
import static org.apache.felix.scr.annotations.ReferencePolicy.STATIC;
import static org.apache.felix.scr.annotations.ReferencePolicyOption.GREEDY;

import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.PropertyOption;
import org.apache.felix.scr.annotations.Reference;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStoreService;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.failover.client.FailoverClient;
import org.apache.jackrabbit.oak.plugins.segment.failover.server.FailoverServer;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Component(policy = ConfigurationPolicy.REQUIRE)
public class FailoverStoreService {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private static final String MODE_MASTER = "master";
    private static final String MODE_SLAVE = "slave";

    public static final String MODE_DEFAULT = MODE_MASTER;
    @Property(label = "Mode", description = "TarMK Failover mode (master or slave)", options = {
            @PropertyOption(name = "master", value = "master"),
            @PropertyOption(name = "slave", value = "slave") }, value = MODE_DEFAULT)
    public static final String MODE = "mode";

    public static final int PORT_DEFAULT = 8023;
    @Property(label = "Port", description = "TarMK Failover port", intValue = PORT_DEFAULT)
    public static final String PORT = "port";

    public static final String MASTER_HOST_DEFAULT = "127.0.0.1";
    @Property(label = "Master Host", description = "TarMK Failover master host (enabled for slave mode only)", value = MASTER_HOST_DEFAULT)
    public static final String MASTER_HOST = "master.host";

    public static final int INTERVAL_DEFAULT = 5;
    @Property(label = "Sync interval (seconds)", description = "TarMK Failover sync interval (seconds)", intValue = INTERVAL_DEFAULT)
    public static final String INTERVAL = "interval";

    public static final String ALLOWED_CLIENT_IP_RANGES_DEFAULT = null;
    @Property(label = "Client allowed IP-Ranges", description = "accept incoming requests for these IP-ranges only")
    public static final String ALLOWED_CLIENT_IP_RANGES = "master.allowed-client-ip-ranges";

    @Reference(policy = STATIC, policyOption = GREEDY)
    private NodeStore store = null;
    private SegmentStore segmentStore;

    private FailoverServer master = null;
    private FailoverClient sync = null;

    private ServiceRegistration syncReg = null;

    @Activate
    private void activate(ComponentContext context) throws IOException {
        if (store instanceof SegmentNodeStoreService) {
            segmentStore = ((SegmentNodeStoreService) store).getSegmentStore();
        } else {
            throw new IllegalArgumentException(
                    "Unexpected NodeStore impl, expecting SegmentNodeStoreService, got "
                            + store.getClass());
        }
        String mode = valueOf(context.getProperties().get(MODE));
        if (MODE_MASTER.equals(mode)) {
            bootstrapMaster(context);
        } else if (MODE_SLAVE.equals(mode)) {
            bootstrapSlave(context);
        } else {
            throw new IllegalArgumentException(
                    "Unexpected 'mode' param, expecting 'master' or 'slave' got "
                            + mode);
        }
    }

    @Deactivate
    public synchronized void deactivate() {
        if (master != null) {
            master.close();
        }
        if (sync != null) {
            sync.close();
        }
        if (syncReg != null) {
            syncReg.unregister();
        }
    }

    private void bootstrapMaster(ComponentContext context) {
        Dictionary<?, ?> props = context.getProperties();
        int port = PropertiesUtil.toInteger(props.get(PORT), PORT_DEFAULT);
        String ipRanges = PropertiesUtil.toString(props.get(ALLOWED_CLIENT_IP_RANGES), ALLOWED_CLIENT_IP_RANGES_DEFAULT);
        String[] ranges = ipRanges == null ? null : ipRanges.split(",");
        master = new FailoverServer(port, segmentStore, ranges);
        master.start();
        log.info("started failover master on port {} with allowed ip ranges {}.", port, ipRanges);
    }

    private void bootstrapSlave(ComponentContext context) {
        Dictionary<?, ?> props = context.getProperties();
        int port = PropertiesUtil.toInteger(props.get(PORT), PORT_DEFAULT);
        long interval = PropertiesUtil.toInteger(props.get(INTERVAL), INTERVAL_DEFAULT);
        String host = PropertiesUtil.toString(props.get(MASTER_HOST), MASTER_HOST_DEFAULT);

        sync = new FailoverClient(host, port, segmentStore);
        Dictionary<Object, Object> dictionary = new Hashtable<Object, Object>();
        dictionary.put("scheduler.period", interval);
        dictionary.put("scheduler.concurrent", false);
        dictionary.put("scheduler.runOn", "SINGLE");

        syncReg = context.getBundleContext().registerService(
                Runnable.class.getName(), sync, dictionary);
        log.info("started failover slave sync with {}:{} at {} sec.", host,
                port, interval);
    }
}
