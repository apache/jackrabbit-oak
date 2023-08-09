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

import static org.apache.sling.testing.mock.osgi.MockOsgi.deactivate;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Dictionary;
import java.util.Hashtable;

import org.apache.jackrabbit.oak.spi.state.NodeStoreProvider;
import org.apache.sling.testing.mock.osgi.MockOsgi;

public class SegmentNodeStoreFactoryTest extends SegmentNodeStoreServiceTest {

    private SegmentNodeStoreFactory segmentNodeStoreFactory;

    @Override
    protected void registerSegmentNodeStoreService(boolean customBlobStore) {
        Hashtable<String, Object> properties = new Hashtable<>();

        properties.put("role", "some-role");
        properties.put("customBlobStore", customBlobStore);
        properties.put("repository.home", folder.getRoot().getAbsolutePath());

        // OAK-10367: The call 
        // context.registerInjectActivateService(new SegmentNodeStoreFactory(), properties)
        // isn't working properly anymore. It calls
        // context.bundleContext().registerService(null, new SegmentNodeStoreFactory(), properties).
        // A service registered this way will not be found by
        // context.bundleContext().getServiceReferences(SegmentNodeStoreService.class, null).
        // 
        //segmentNodeStoreFactory = context.registerInjectActivateService(new SegmentNodeStoreFactory(), properties);
        segmentNodeStoreFactory = new SegmentNodeStoreFactory();
        MockOsgi.injectServices(segmentNodeStoreFactory, context.bundleContext(), properties);
        MockOsgi.activate(segmentNodeStoreFactory, context.bundleContext(), (Dictionary<String, Object>) properties);
        context.bundleContext().registerService(SegmentNodeStoreFactory.class, segmentNodeStoreFactory, properties);
    }

    @Override
    protected void unregisterSegmentNodeStoreService() {
        deactivate(segmentNodeStoreFactory, context.bundleContext());
    }

    @Override
    protected void assertServiceActivated() {
        assertNotNull(context.getService(NodeStoreProvider.class));
    }

    @Override
    protected void assertServiceNotActivated() {
        assertNull(context.getService(NodeStoreProvider.class));
    }

}
