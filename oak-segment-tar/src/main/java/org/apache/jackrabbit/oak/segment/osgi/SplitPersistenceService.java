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
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.spi.persistence.split.SplitPersistence;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;

import java.io.IOException;
import java.util.Properties;

import static org.osgi.framework.Constants.SERVICE_PID;
import static org.osgi.service.component.annotations.ReferencePolicyOption.GREEDY;

@Component
public class SplitPersistenceService {

    @Reference(policyOption = GREEDY, target = "(role=split-persistence-ro)")
    @SuppressWarnings("unused")
    private SegmentNodeStorePersistence roPersistence;

    @Reference(policyOption = GREEDY, target = "(role=split-persistence-rw)")
    @SuppressWarnings("unused")
    private SegmentNodeStorePersistence rwPersistence;

    private volatile Registration registration;


    @Activate
    @SuppressWarnings("unused")
    void activate(BundleContext bundleContext) throws IOException {
        final OsgiWhiteboard whiteboard = new OsgiWhiteboard(bundleContext);
        registration = whiteboard.register(
                SegmentNodeStorePersistence.class,
                new SplitPersistence(roPersistence, rwPersistence),
                new Properties() {{
                    put(SERVICE_PID, String.format("%s(%s, %s)",
                            SplitPersistence.class.getName(),
                            roPersistence.getClass().getSimpleName(),
                            rwPersistence.getClass().getSimpleName()));
                    put("role", "split-persistence");
                }});
    }

    @Deactivate
    @SuppressWarnings("unused")
    void deactivate() {
        registration.unregister();
    }
}
