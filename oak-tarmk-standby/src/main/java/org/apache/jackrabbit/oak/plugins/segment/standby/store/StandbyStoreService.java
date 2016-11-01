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
package org.apache.jackrabbit.oak.plugins.segment.standby.store;

import static java.lang.String.valueOf;
import static org.apache.felix.scr.annotations.ReferencePolicy.STATIC;
import static org.apache.felix.scr.annotations.ReferencePolicyOption.GREEDY;

import java.io.IOException;
import java.security.cert.CertificateException;
import java.util.Dictionary;
import java.util.Hashtable;

import javax.net.ssl.SSLException;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.PropertyOption;
import org.apache.felix.scr.annotations.Reference;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStoreProvider;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.standby.client.StandbyClient;
import org.apache.jackrabbit.oak.plugins.segment.standby.server.StandbyServer;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Property(name = "org.apache.sling.installer.configuration.persist", label="Persist configuration", description = "Must be always disabled to avoid storing the configuration in the repository", boolValue = false)
@Component(metatype = true, policy = ConfigurationPolicy.REQUIRE)
@Deprecated
public class StandbyStoreService {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private static final String MODE_PRIMARY = "primary";
    private static final String MODE_STANDBY = "standby";

    @Deprecated
    public static final String MODE_DEFAULT = MODE_PRIMARY;
    @Property(options = {
            @PropertyOption(name = MODE_PRIMARY, value = MODE_PRIMARY),
            @PropertyOption(name = MODE_STANDBY, value = MODE_STANDBY) },
            value = MODE_DEFAULT)
    @Deprecated
    public static final String MODE = "mode";

    @Deprecated
    public static final int PORT_DEFAULT = 8023;
    @Property(intValue = PORT_DEFAULT)
    @Deprecated
    public static final String PORT = "port";

    @Deprecated
    public static final String PRIMARY_HOST_DEFAULT = "127.0.0.1";
    @Property(value = PRIMARY_HOST_DEFAULT)
    @Deprecated
    public static final String PRIMARY_HOST = "primary.host";

    @Deprecated
    public static final int INTERVAL_DEFAULT = 5;
    @Property(intValue = INTERVAL_DEFAULT)
    @Deprecated
    public static final String INTERVAL = "interval";

    @Deprecated
    public static final String[] ALLOWED_CLIENT_IP_RANGES_DEFAULT = new String[] {};
    @Property(cardinality = Integer.MAX_VALUE)
    @Deprecated
    public static final String ALLOWED_CLIENT_IP_RANGES = "primary.allowed-client-ip-ranges";

    @Deprecated
    public static final boolean SECURE_DEFAULT = false;
    @Property(boolValue = SECURE_DEFAULT)
    @Deprecated
    public static final String SECURE = "secure";

    @Deprecated
    public static final int READ_TIMEOUT_DEFAULT = 60000;
    @Property(intValue = READ_TIMEOUT_DEFAULT)
    @Deprecated
    public static final String READ_TIMEOUT = "standby.readtimeout";

    @Deprecated
    public static final boolean AUTO_CLEAN_DEFAULT = false;
    @Property(boolValue = AUTO_CLEAN_DEFAULT)
    @Deprecated
    public static final String AUTO_CLEAN = "standby.autoclean";

    @Reference(policy = STATIC, policyOption = GREEDY)
    private SegmentStoreProvider storeProvider = null;

    private SegmentStore segmentStore;

    private StandbyServer primary = null;
    private StandbyClient sync = null;

    private ServiceRegistration syncReg = null;

    @Activate
    private void activate(ComponentContext context) throws IOException, CertificateException {
        if (storeProvider != null) {
            segmentStore = storeProvider.getSegmentStore();
        } else {
            throw new IllegalArgumentException(
                    "Missing SegmentStoreProvider service");
        }
        String mode = valueOf(context.getProperties().get(MODE));
        if (MODE_PRIMARY.equals(mode)) {
            bootstrapMaster(context);
        } else if (MODE_STANDBY.equals(mode)) {
            bootstrapSlave(context);
        } else {
            throw new IllegalArgumentException(
                    "Unexpected 'mode' param, expecting 'primary' or 'standby' got "
                            + mode);
        }
    }

    @Deactivate
    @Deprecated
    public synchronized void deactivate() {
        if (primary != null) {
            primary.close();
        }
        if (sync != null) {
            sync.close();
        }
        if (syncReg != null) {
            syncReg.unregister();
        }
    }

    private void bootstrapMaster(ComponentContext context) throws CertificateException, SSLException {
        Dictionary<?, ?> props = context.getProperties();
        int port = PropertiesUtil.toInteger(props.get(PORT), PORT_DEFAULT);
        String[] ranges = PropertiesUtil.toStringArray(props.get(ALLOWED_CLIENT_IP_RANGES), ALLOWED_CLIENT_IP_RANGES_DEFAULT);
        boolean secure = PropertiesUtil.toBoolean(props.get(SECURE), SECURE_DEFAULT);
        primary = new StandbyServer(port, segmentStore, ranges, secure);
        primary.start();
        log.info("started primary on port {} with allowed ip ranges {}.", port, ranges);
    }

    private void bootstrapSlave(ComponentContext context) throws SSLException {
        Dictionary<?, ?> props = context.getProperties();
        int port = PropertiesUtil.toInteger(props.get(PORT), PORT_DEFAULT);
        long interval = PropertiesUtil.toInteger(props.get(INTERVAL), INTERVAL_DEFAULT);
        String host = PropertiesUtil.toString(props.get(PRIMARY_HOST), PRIMARY_HOST_DEFAULT);
        boolean secure = PropertiesUtil.toBoolean(props.get(SECURE), SECURE_DEFAULT);
        int readTimeout = PropertiesUtil.toInteger(props.get(READ_TIMEOUT), READ_TIMEOUT_DEFAULT);
        boolean clean = PropertiesUtil.toBoolean(props.get(AUTO_CLEAN), AUTO_CLEAN_DEFAULT);

        sync = new StandbyClient(host, port, segmentStore, secure, readTimeout, clean);
        Dictionary<Object, Object> dictionary = new Hashtable<Object, Object>();
        dictionary.put("scheduler.period", interval);
        dictionary.put("scheduler.concurrent", false);
        // dictionary.put("scheduler.runOn", "SINGLE");

        syncReg = context.getBundleContext().registerService(
                Runnable.class.getName(), sync, dictionary);
        log.info("started standby sync with {}:{} at {} sec.", host,
                port, interval);
    }
}
