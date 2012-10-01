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
package org.apache.jackrabbit.oak.osgi;

import org.apache.jackrabbit.oak.spi.commit.CompositeValidatorProvider;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

import java.util.HashMap;
import java.util.Map;

/**
 * This validator provider combines all validators of all available OSGi validator
 * providers.
 */
public class OsgiValidatorProvider implements ServiceTrackerCustomizer, ValidatorProvider {

    private BundleContext context;

    private ServiceTracker tracker;

    private final Map<ServiceReference, ValidatorProvider> providers =
        new HashMap<ServiceReference, ValidatorProvider>();

    public void start(BundleContext bundleContext) throws Exception {
        context = bundleContext;
        tracker = new ServiceTracker(
                bundleContext, ValidatorProvider.class.getName(), this);
        tracker.open();
    }

    public void stop() throws Exception {
        tracker.close();
    }

    //------------------------------------------------------------< ServiceTrackerCustomizer >---

    @Override
    public Object addingService(ServiceReference reference) {
        Object service = context.getService(reference);
        if (service instanceof ValidatorProvider) {
            ValidatorProvider provider = (ValidatorProvider) service;
            providers.put(reference, provider);
            return service;
        } else {
            context.ungetService(reference);
            return null;
        }
    }

    @Override
    public void modifiedService(ServiceReference reference, Object service) {
        // nothing to do
    }

    @Override
    public void removedService(ServiceReference reference, Object service) {
        providers.remove(reference);
        context.ungetService(reference);
    }

    //------------------------------------------------------------< ValidatorProvider >---

    @Override
    public Validator getRootValidator(NodeState before, NodeState after) {
        return CompositeValidatorProvider.compose(providers.values())
                .getRootValidator(before, after);
    }
}
