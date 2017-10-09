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
package org.apache.jackrabbit.oak.segment;

import java.util.Map;

import org.apache.jackrabbit.oak.plugins.blob.AbstractBlobGCRegistrationTest;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;

import static com.google.common.collect.Maps.newHashMap;
import static org.apache.sling.testing.mock.osgi.MockOsgi.deactivate;

public class SegmentBlobGCRegistrationTest extends AbstractBlobGCRegistrationTest {

    private SegmentNodeStoreService service;

    @Override
    protected void registerNodeStoreService() {
        Map<String, Object> properties = newHashMap();
        properties.put(SegmentNodeStoreService.CUSTOM_BLOB_STORE, true);
        properties.put(SegmentNodeStoreService.REPOSITORY_HOME_DIRECTORY, repoHome);
        service = context.registerInjectActivateService(new SegmentNodeStoreService(), properties);
    }

    @Override
    protected void unregisterNodeStoreService() {
        ServiceReference[] serviceReferences;
        try {
            serviceReferences = context.bundleContext().getServiceReferences(SegmentNodeStoreService.class.getName(), null);
        } catch (InvalidSyntaxException e) {
            throw new IllegalStateException("Unable to read references to SegmentNodeStoreService", e);
        }
        for (ServiceReference serviceReference : serviceReferences) {
            Object service = context.bundleContext().getService(serviceReference);
            if (service == null) {
                continue;
            }
            deactivate(service, serviceReference.getBundle().getBundleContext());
        }
    }
}
