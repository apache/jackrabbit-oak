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

package org.apache.jackrabbit.mongomk.osgi;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Property;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mongomk.impl.BlobStoreMongo;
import org.apache.jackrabbit.mongomk.impl.MongoConnection;
import org.apache.jackrabbit.mongomk.impl.MongoMicroKernel;
import org.apache.jackrabbit.mongomk.impl.NodeStoreMongo;
import org.apache.jackrabbit.mongomk.util.MongoUtil;
import org.apache.sling.commons.osgi.PropertiesUtil;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Deactivate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;


@Component(metatype = true,
        label = "%oak.mongomk.label",
        description = "%oak.mongomk.description",
        policy = ConfigurationPolicy.REQUIRE
)
public class MongoMicroKernalService {

    private static final String DEFAULT_HOST = "localhost";

    private static final int DEFAULT_PORT= 27017;

    private static final String DEFAULT_DB = "oak";


    @Property(value = DEFAULT_HOST)
    private static final String PROP_HOST = "host";

    @Property(intValue = DEFAULT_PORT)
    private static final String PROP_PORT = "port";

    @Property(value = DEFAULT_DB)
    private static final String PROP_DB = "db";

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private ServiceRegistration reg;

    private MongoConnection connection;

    @Activate
    private void activate(BundleContext context,Map<String,?> config) throws Exception {
        String host = PropertiesUtil.toString(config.get(PROP_HOST), DEFAULT_HOST);
        int port = PropertiesUtil.toInteger(config.get(PROP_PORT), DEFAULT_PORT);
        String db = PropertiesUtil.toString(config.get(PROP_DB), DEFAULT_DB);

        logger.info("Starting MongoDB MicroKernal with host={}, port={}, db={}",
                new Object[] {host, port, db});
        connection = new MongoConnection(host,port,db);

        MongoUtil.bootstrap(connection);
        logger.info("Connected to database {}", connection.getDB());

        MongoMicroKernel mk = new MongoMicroKernel( new NodeStoreMongo(connection),new BlobStoreMongo(connection));

        Properties props = new Properties();
        props.setProperty("oak.mk.type","mongo");
        reg = context.registerService(MicroKernel.class.getName(),mk,props);
    }

    @Deactivate
    private void deactivate(){
        if(reg != null){
            reg.unregister();
        }

        if(connection != null){
            connection.close();
        }
    }

}
