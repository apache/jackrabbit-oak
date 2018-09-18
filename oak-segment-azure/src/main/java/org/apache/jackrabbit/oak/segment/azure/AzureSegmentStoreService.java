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

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
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
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Properties;

@Component(
        configurationPolicy = ConfigurationPolicy.REQUIRE,
        configurationPid = {Configuration.PID})
public class AzureSegmentStoreService {

    private static final Logger log = LoggerFactory.getLogger(AzureSegmentStoreService.class);

    public static final String DEFAULT_CONTAINER_NAME = "oak";

    public static final String DEFAULT_ROOT_PATH = "/oak";

    private ServiceRegistration registration;

    private SegmentNodeStorePersistence persistence;

    @Activate
    public void activate(ComponentContext context, Configuration config) throws IOException {
        persistence = createAzurePersistence(config);
        registration = context.getBundleContext().registerService(SegmentNodeStorePersistence.class.getName(), persistence, new Properties());
    }

    @Deactivate
    public void deactivate() throws IOException {
        if (registration != null) {
            registration.unregister();
            registration = null;
        }
        persistence = null;
    }

    private static SegmentNodeStorePersistence createAzurePersistence(Configuration configuration) throws IOException {
        try {
            StringBuilder connectionString = new StringBuilder();
            if (configuration.connectionURL() == null || configuration.connectionURL().trim().isEmpty()) {
                connectionString.append("DefaultEndpointsProtocol=https;");
                connectionString.append("AccountName=").append(configuration.accountName()).append(';');
                connectionString.append("AccountKey=").append(configuration.accessKey()).append(';');
            } else {
                connectionString.append(configuration.connectionURL());
            }
            log.info("Connection string: '{}'", connectionString.toString());
            CloudStorageAccount cloud = CloudStorageAccount.parse(connectionString.toString());
            CloudBlobContainer container = cloud.createCloudBlobClient().getContainerReference(configuration.containerName());
            container.createIfNotExists();

            String path = configuration.rootPath();
            if (path != null && path.length() > 0 && path.charAt(0) == '/') {
                path = path.substring(1);
            }

            AzurePersistence persistence = new AzurePersistence(container.getDirectoryReference(path));
            return persistence;
        } catch (StorageException | URISyntaxException | InvalidKeyException e) {
            throw new IOException(e);
        }
    }

}

