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
package org.apache.jackrabbit.oak.segment.azure;

import org.apache.jackrabbit.oak.segment.azure.v8.AzurePersistenceV8;
import org.apache.jackrabbit.oak.segment.azure.v8.AzureSegmentStoreV8;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Objects;

import static org.osgi.framework.Constants.SERVICE_PID;

@Component(
        configurationPolicy = ConfigurationPolicy.REQUIRE,
        configurationPid = {Configuration.PID})
public class AzureSegmentStoreService {

    private static final Logger log = LoggerFactory.getLogger(AzureSegmentStoreService.class);

    public static final String DEFAULT_CONTAINER_NAME = "oak";

    public static final String DEFAULT_ROOT_PATH = "/oak";

    public static final boolean DEFAULT_ENABLE_SECONDARY_LOCATION = false;

    private ServiceRegistration registration;

    public static final String SEGMENT_AZURE_V_12_ENABLED = "segment.azure.v12.enabled";

    private final boolean useAzureSdkV12 = Boolean.getBoolean(SEGMENT_AZURE_V_12_ENABLED);


    @Activate
    public void activate(ComponentContext context, Configuration config) throws IOException {
        if (useAzureSdkV12) {
            AzurePersistence persistence;
            String accountName;
            if (config.failoverEnabled()) {
                log.info("Starting node store using Azure SDK 12 in failover mode");
                accountName = config.failoverAccountName();
                persistence = AzurePersistenceManager.createAzurePersistenceFromFailover(config);
            } else {
                log.info("Starting node store using Azure SDK 12");
                accountName = config.accountName();
                persistence = AzurePersistenceManager.createAzurePersistenceFrom(config);
            }
            Hashtable<String, Object> properties = getServiceRegistrationProperties(AzurePersistence.class.getName(), config, accountName);

            registration = context.getBundleContext().registerService(SegmentNodeStorePersistence.class, persistence, properties);
        } else {
            log.info("Starting node store using Azure SDK 8");
            Hashtable<String, Object> properties = getServiceRegistrationProperties(AzurePersistenceV8.class.getName(), config, config.accountName());
            AzurePersistenceV8 persistence = AzureSegmentStoreV8.createAzurePersistenceFrom(config);
            registration = context.getBundleContext().registerService(SegmentNodeStorePersistence.class, persistence, properties);
        }
    }

    private Hashtable<String, Object> getServiceRegistrationProperties(String persistenceClassName, Configuration config, String accountName) {
        Hashtable<String, Object> properties = new Hashtable<>();
        properties.put(SERVICE_PID, String.format("%s(%s, %s)", persistenceClassName, accountName, config.rootPath()));
        if (!Objects.equals(config.role(), "")) {
            properties.put("role", config.role());
        }
        return properties;
    }

    @Deactivate
    public void deactivate() throws IOException {
        if (registration != null) {
            registration.unregister();
            registration = null;
        }
    }

}