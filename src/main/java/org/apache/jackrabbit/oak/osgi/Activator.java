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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.plugins.nodetype.DefaultTypeEditor;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.ValidatingHook;
import org.apache.jackrabbit.oak.spi.lifecycle.OakInitializer;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

public class Activator implements BundleActivator, ServiceTrackerCustomizer {

    private BundleContext context;

    private ServiceTracker tracker;

    private final OsgiIndexProvider indexProvider = new OsgiIndexProvider();

    private final OsgiIndexHookProvider indexHookProvider = new OsgiIndexHookProvider();

    private final OsgiValidatorProvider validatorProvider = new OsgiValidatorProvider();

    private final OsgiRepositoryInitializer kernelTracker = new OsgiRepositoryInitializer();

    private final Map<ServiceReference, ServiceRegistration> services =
            new HashMap<ServiceReference, ServiceRegistration>();

    //----------------------------------------------------< BundleActivator >---

    @Override
    public void start(BundleContext bundleContext) throws Exception {
        context = bundleContext;

        indexProvider.start(bundleContext);
        indexHookProvider.start(bundleContext);
        validatorProvider.start(bundleContext);
        kernelTracker.start(bundleContext);

        tracker = new ServiceTracker(
                context, MicroKernel.class.getName(), this);
        tracker.open();
    }

    @Override
    public void stop(BundleContext bundleContext) throws Exception {
        tracker.close();

        indexProvider.stop();
        indexHookProvider.stop();
        validatorProvider.stop();
        kernelTracker.stop();
    }

    //-------------------------------------------< ServiceTrackerCustomizer >---

    @Override
    public Object addingService(ServiceReference reference) {
        Object service = context.getService(reference);
        if (service instanceof MicroKernel) {
            MicroKernel kernel = (MicroKernel) service;
            OakInitializer.initialize(new KernelNodeStore(kernel),
                    kernelTracker, indexHookProvider);
            Oak oak = new Oak(kernel)
                    .with(new CompositeHook(
                        // TODO: DefaultTypeEditor is JCR specific and does not belong here
                        new DefaultTypeEditor(),
                        new ValidatingHook(validatorProvider)))
                    .with(indexProvider)
                    .with(indexHookProvider);
            services.put(reference, context.registerService(
                    ContentRepository.class.getName(),
                    oak.createContentRepository(),
                    new Properties()));
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
        ServiceRegistration registration = services.remove(reference);
        registration.unregister();
        context.ungetService(reference);
    }

}
