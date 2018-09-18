/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.osgi;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.junit.Test;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;

public class OsgiWhiteboardTest {

    /**
     * OAK-3409
     */
    @Test
    public void testDoubleUnregister() {
        BundleContext bundleContext = mock(BundleContext.class);
        OsgiWhiteboard w = new OsgiWhiteboard(bundleContext);

        Runnable r = new Runnable() {
            @Override
            public void run() {
                //
            }
        };

        final AtomicBoolean unregistered = new AtomicBoolean();
        ServiceRegistration sr = new ServiceRegistration() {

            @Override
            public void unregister() {
                if (unregistered.get()) {
                    throw new IllegalStateException(
                            "Service already unregistered.");
                }
                unregistered.set(true);
            }

            @Override
            public void setProperties(Dictionary properties) {
            }

            @Override
            public ServiceReference getReference() {
                return null;
            }
        };

        when(
                bundleContext.registerService(Runnable.class.getName(), r,
                        new Hashtable<Object, Object>())).thenReturn(sr);
        Registration reg = w.register(Runnable.class, r,
                new HashMap<String, Object>());
        reg.unregister();

        assertTrue(unregistered.get());
        reg.unregister();
    }

}
