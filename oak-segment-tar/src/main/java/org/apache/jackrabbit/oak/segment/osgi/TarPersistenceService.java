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
package org.apache.jackrabbit.oak.segment.osgi;

import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.osgi.framework.Constants.SERVICE_PID;

@Component(configurationPolicy = ConfigurationPolicy.REQUIRE)
@Designate(ocd = TarPersistenceService.Configuration.class)
public class TarPersistenceService {

    @ObjectClassDefinition(
            name = "Oak TAR segment persistence service",
            description = "Apache Jackrabbit Oak SegmentPersistence implementation" +
                "used to configure a TAR persistence.")
    public @interface Configuration {
        @AttributeDefinition(
                name = "Storage Directory",
                description = "Path on the file system where this TAR persistence stores its files.")
        String storage_directory();

        @AttributeDefinition(
                name = "Role",
                description = "The role of this persistence. It should be unique and may be used to filter " +
                        "services in order to create services composed of multiple persistence instances. " +
                        "E.g. a SplitPersistence composed of a TAR persistence and an Azure persistence.")
        String role() default "";
    }

    private volatile Registration registration;

    @Activate
    @SuppressWarnings("unused")
    void activate(BundleContext bundleContext, Configuration configuration) {
        OsgiWhiteboard whiteboard = new OsgiWhiteboard(bundleContext);
        final String storageDirectory = configuration.storage_directory();
        final File directory = new File(storageDirectory);
        final TarPersistence tarPersistence = new TarPersistence(directory);

        final Map<Object, Object> properties = new HashMap<>();
        properties.put(SERVICE_PID, String.format("%s(%s)", TarPersistence.class.getName(), storageDirectory));
        if (!Objects.equals(configuration.role(), "")) {
            properties.put("role", configuration.role());
        }
        registration = whiteboard.register(SegmentNodeStorePersistence.class, tarPersistence, properties);
    }

    @Deactivate
    @SuppressWarnings("unused")
    void deactivate() {
        registration.unregister();
    }
}
