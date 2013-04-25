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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.core.ContentRepositoryImpl;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.osgi.OsgiRepositoryInitializer.RepositoryInitializerObserver;
import org.apache.jackrabbit.oak.security.SecurityProviderImpl;
import org.apache.jackrabbit.oak.spi.lifecycle.OakInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

public class Activator implements BundleActivator, ServiceTrackerCustomizer, RepositoryInitializerObserver {

    private BundleContext context;

    private ServiceTracker microKernelTracker;

    // see OAK-795 for a reason why the nodeStore tracker is disabled 
    // private ServiceTracker nodeStoreTracker;

    private final OsgiIndexProvider indexProvider = new OsgiIndexProvider();

    private final OsgiIndexEditorProvider indexEditorProvider = new OsgiIndexEditorProvider();

    private final OsgiEditorProvider validatorProvider = new OsgiEditorProvider();

    private final OsgiRepositoryInitializer repositoryInitializerTracker = new OsgiRepositoryInitializer();

    private final Map<ServiceReference, ServiceRegistration> services = new HashMap<ServiceReference, ServiceRegistration>();

    //----------------------------------------------------< BundleActivator >---

    @Override
    public void start(BundleContext bundleContext) throws Exception {
        context = bundleContext;

        indexProvider.start(bundleContext);
        indexEditorProvider.start(bundleContext);
        validatorProvider.start(bundleContext);
        repositoryInitializerTracker.setObserver(this);
        repositoryInitializerTracker.start(bundleContext);
        microKernelTracker = new ServiceTracker(
                context, MicroKernel.class.getName(), this);
        microKernelTracker.open();
        // nodeStoreTracker = new ServiceTracker(
        // context, NodeStore.class.getName(), this);
        // nodeStoreTracker.open();
    }

    @Override
    public void stop(BundleContext bundleContext) throws Exception {
        // nodeStoreTracker.close();
        microKernelTracker.close();
        indexProvider.stop();
        indexEditorProvider.stop();
        validatorProvider.stop();
        repositoryInitializerTracker.stop();
    }

    //-------------------------------------------< ServiceTrackerCustomizer >---

    @Override
    public Object addingService(ServiceReference reference) {
        Object service = context.getService(reference);
        if (service instanceof MicroKernel) {
            MicroKernel kernel = (MicroKernel) service;
            services.put(reference, context.registerService(
                    NodeStore.class.getName(),
                    new KernelNodeStore(kernel),
                    new Properties()));
        } else if (service instanceof NodeStore) {
            NodeStore store = (NodeStore) service;
            OakInitializer.initialize(store, repositoryInitializerTracker, indexEditorProvider);
            Oak oak = new Oak(store)
                // FIXME: proper osgi setup for security provider (see OAK-17 and sub-tasks)
                .with(new SecurityProviderImpl())
                .with(validatorProvider)
                .with(indexProvider)
                .with(indexEditorProvider);
            services.put(reference, context.registerService(
                    ContentRepository.class.getName(),
                    oak.createContentRepository(),
                    new Properties()));
        }
        return service;
    }

    @Override
    public void modifiedService(ServiceReference reference, Object service) {
        // nothing to do
    }

    @Override
    public void removedService(ServiceReference reference, Object service) {
        services.remove(reference).unregister();
        context.ungetService(reference);
    }

    //----------------------------------------< RepositoryInitializerObserver >---

    @Override
    public void newRepositoryInitializer(RepositoryInitializer ri) {
        List<ServiceReference> mkRefs = new ArrayList<ServiceReference>(services.keySet());
        for (ServiceReference ref : mkRefs) {
            Object service = context.getService(ref);
            if (service instanceof ContentRepositoryImpl) {
                ContentRepositoryImpl repository = (ContentRepositoryImpl) service;
                OakInitializer.initialize(repository.getNodeStore(), ri,
                        indexEditorProvider);
            }
        }
    }

}
