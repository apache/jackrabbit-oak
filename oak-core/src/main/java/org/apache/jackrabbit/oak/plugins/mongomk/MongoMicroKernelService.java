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

package org.apache.jackrabbit.oak.plugins.mongomk;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Property;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.plugins.mongomk.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.whiteboard.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.sling.commons.osgi.PropertiesUtil;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Deactivate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;

import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

/**
 * The OSGi service to start/stop a MongoMK instance.
 */
@Component(metatype = true,
        label = "%oak.mongomk.label",
        description = "%oak.mongomk.description",
        policy = ConfigurationPolicy.REQUIRE
)
public class MongoMicroKernelService {

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 27017;
    private static final String DEFAULT_DB = "oak";
    private static final int DEFAULT_CACHE = 256;

    @Property(value = DEFAULT_HOST)
    private static final String PROP_HOST = "host";

    @Property(intValue = DEFAULT_PORT)
    private static final String PROP_PORT = "port";

    @Property(value = DEFAULT_DB)
    private static final String PROP_DB = "db";

    @Property(intValue = DEFAULT_CACHE)
    private static final String PROP_CACHE = "cache";
    private static final long MB = 1024 * 1024;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private ServiceRegistration reg;
    private MongoMK mk;
    private final List<Registration> registrations = new ArrayList<Registration>();

    @Activate
    private void activate(BundleContext context, Map<String, ?> config)
            throws Exception {
        String host = PropertiesUtil.toString(config.get(PROP_HOST), DEFAULT_HOST);
        int port = PropertiesUtil.toInteger(config.get(PROP_PORT), DEFAULT_PORT);
        String db = PropertiesUtil.toString(config.get(PROP_DB), DEFAULT_DB);
        int cacheSize = PropertiesUtil.toInteger(config.get(PROP_CACHE), DEFAULT_CACHE);

        logger.info("Starting MongoDB MicroKernel with host={}, port={}, db={}",
                new Object[] {host, port, db});

        MongoConnection connection = new MongoConnection(host, port, db);
        DB mongoDB = connection.getDB();

        logger.info("Connected to database {}", mongoDB);

        mk = new MongoMK.Builder()
                        .memoryCacheSize(cacheSize * MB)
                        .setMongoDB(mongoDB)
                        .open();

        registerJMXBeans(mk, context);

        reg = context.registerService(MicroKernel.class.getName(), mk, new Properties());
    }

    private void registerJMXBeans(MongoMK mk, BundleContext context) {
        Whiteboard wb = new OsgiWhiteboard(context);
        registrations.add(
            registerMBean(wb,
                    CacheStatsMBean.class,
                    mk.getNodeCacheStats(),
                    CacheStatsMBean.TYPE,
                    mk.getNodeCacheStats().getName())
        );
        registrations.add(
            registerMBean(wb,
                    CacheStatsMBean.class,
                    mk.getNodeChildrenCacheStats(),
                    CacheStatsMBean.TYPE,
                    mk.getNodeChildrenCacheStats().getName())
        );
        registrations.add(
            registerMBean(wb,
                    CacheStatsMBean.class,
                    mk.getDiffCacheStats(),
                    CacheStatsMBean.TYPE,
                    mk.getDiffCacheStats().getName())
        );

        DocumentStore ds = mk.getDocumentStore();
        if (ds instanceof MongoDocumentStore) {
            MongoDocumentStore mds = (MongoDocumentStore) ds;
            registrations.add(
                    registerMBean(wb,
                            CacheStatsMBean.class,
                            mds.getCacheStats(),
                            CacheStatsMBean.TYPE,
                            mds.getCacheStats().getName())
            );
        }
    }

    @Deactivate
    private void deactivate() {
        for (Registration r : registrations) {
            r.unregister();
        }

        if (reg != null) {
            reg.unregister();
        }

        if (mk != null) {
            mk.dispose();
        }
    }
}