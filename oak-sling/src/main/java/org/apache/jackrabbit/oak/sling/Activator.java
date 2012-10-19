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
package org.apache.jackrabbit.oak.sling;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.jcr.Repository;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.sling.jcr.api.SlingRepository;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

public class Activator implements BundleActivator, ServiceTrackerCustomizer {

    private BundleContext context;

    private ScheduledExecutorService executor;

    private SecurityProvider securityProvider;

    private ServiceTracker tracker;

    private final Map<ServiceReference, ServiceRegistration> jcrRepositories =
            new HashMap<ServiceReference, ServiceRegistration>();

    private final Map<ServiceReference, ServiceRegistration> slingRepositories =
            new HashMap<ServiceReference, ServiceRegistration>();

    //-----------------------------------------------------< BundleActivator >--

    @Override
    public void start(BundleContext bundleContext) throws Exception {
        context = bundleContext;
        executor = Executors.newScheduledThreadPool(1);
        securityProvider = null; // TODO
        tracker = new ServiceTracker(
                context, ContentRepository.class.getName(), this);
        tracker.open();
    }

    @Override
    public void stop(BundleContext bundleContext) throws Exception {
        tracker.close();
        executor.shutdown();
    }

    //--------------------------------------------< ServiceTrackerCustomizer >--

    @Override
    public Object addingService(ServiceReference reference) {
        Object service = context.getService(reference);
        if (service instanceof ContentRepository) {
            SlingRepository repository = new SlingRepositoryImpl(
                    (ContentRepository) service, executor, securityProvider);
            jcrRepositories.put(reference, context.registerService(
                    Repository.class.getName(),
                    repository, new Properties()));
            slingRepositories.put(reference, context.registerService(
                    SlingRepository.class.getName(),
                    repository, new Properties()));
            return service;
        } else {
            context.ungetService(reference);
            return null;
        }
    }

    @Override
    public void modifiedService(ServiceReference reference, Object service) {
    }

    @Override
    public void removedService(ServiceReference reference, Object service) {
        slingRepositories.get(reference).unregister();
        jcrRepositories.get(reference).unregister();
        context.ungetService(reference);
    }

}
