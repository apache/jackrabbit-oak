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

package org.apache.jackrabbit.oak.run.osgi;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.RepositoryFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.SettableFuture;
import de.kalpatec.pojosr.framework.launch.BundleDescriptor;
import de.kalpatec.pojosr.framework.launch.ClasspathScanner;
import de.kalpatec.pojosr.framework.launch.PojoServiceRegistry;
import de.kalpatec.pojosr.framework.launch.PojoServiceRegistryFactory;
import org.apache.commons.io.FilenameUtils;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class OakOSGiRepositoryFactory implements RepositoryFactory {

    private static Logger log = LoggerFactory.getLogger(OakOSGiRepositoryFactory.class);

    /**
     * Name of the repository home parameter.
     */
    public static final String REPOSITORY_HOME
            = "org.apache.jackrabbit.repository.home";

    public static final String REPOSITORY_STARTUP_TIMEOUT
            = "org.apache.jackrabbit.oak.repository.startupTimeOut";

    /**
     * Config key which refers to the map of config where key in that map refers to OSGi
     * config
     */
    public static final String REPOSITORY_CONFIG = "org.apache.jackrabbit.oak.repository.config";

    /**
     * Comma separated list of file names which referred to config stored in form of JSON. The
     * JSON content consist of pid as the key and config map as the value
     */
    public static final String REPOSITORY_CONFIG_FILE = "org.apache.jackrabbit.oak.repository.configFile";

    /**
     * Default timeout for repository creation
     */
    private static final int DEFAULT_TIMEOUT = (int) TimeUnit.MINUTES.toSeconds(10);

    @SuppressWarnings("unchecked")
    public Repository getRepository(Map parameters) throws RepositoryException {
        Map config = new HashMap();
        config.putAll(parameters);

        //TODO With OSGi Whiteboard we need to provide support for handling
        //execution and JMX support as so far they were provided by Sling bundles
        //in OSGi env

        PojoServiceRegistry registry = initializeServiceRegistry(config);

        //Future which would be used to notify when repository is ready
        // to be used
        SettableFuture<Repository> repoFuture = SettableFuture.create();

        //Start the tracker for repository creation
        new RepositoryTracker(registry, repoFuture);

        //Now wait for repository to be created with given timeout
        //if repository creation takes more time. This is required to handle case
        // where OSGi runtime fails to start due to bugs (like cycles)
        int timeout = getTimeoutInSeconds(config);
        try {
            return repoFuture.get(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RepositoryException("Repository initialization was interrupted");
        } catch (ExecutionException e) {
            throw new RepositoryException(e);
        } catch (TimeoutException e) {
            try {
                shutdown(registry);
            } catch (BundleException be) {
                log.warn("Error occurred while shutting down the service registry (due to " +
                        "startup timeout) backing the Repository ", be);
            }
            throw new RepositoryException("Repository could not be started in " +
                    timeout + " seconds", e);
        }
    }

    @SuppressWarnings("unchecked")
    PojoServiceRegistry initializeServiceRegistry(Map config) {
        processConfig(config);

        PojoServiceRegistry registry = createServiceRegistry(config);
        startConfigTracker(registry, config);
        preProcessRegistry(registry);
        startBundles(registry);
        postProcessRegistry(registry);

        return registry;
    }

    /**
     * Enables pre processing of service registry by sub classes. This can be
     * used to register services before any bundle gets started
     *
     * @param registry service registry
     */
    protected void preProcessRegistry(PojoServiceRegistry registry) {

    }

    /**
     * Enables post processing of service registry e.g. registering new services etc
     * by sub classes
     *
     * @param registry service registry
     */
    protected void postProcessRegistry(PojoServiceRegistry registry) {

    }

    protected List<BundleDescriptor> processDescriptors(List<BundleDescriptor> descriptors) {
        Collections.sort(descriptors, new BundleDescriptorComparator());
        return descriptors;
    }

    static void shutdown(PojoServiceRegistry registry) throws BundleException {
        if (registry != null) {
            registry.getBundleContext().getBundle().stop();
        }
    }

    private static void startConfigTracker(PojoServiceRegistry registry, Map config) {
        new ConfigTracker(config, registry.getBundleContext());
    }

    private static int getTimeoutInSeconds(Map config) {
        Integer timeout = (Integer) config.get(REPOSITORY_STARTUP_TIMEOUT);
        if (timeout == null) {
            timeout = DEFAULT_TIMEOUT;
        }
        return timeout;
    }

    @SuppressWarnings("unchecked")
    private static void processConfig(Map config) {
        String home = (String) config.get(REPOSITORY_HOME);
        checkNotNull(home, "Repository home not defined via [%s]", REPOSITORY_HOME);

        home = FilenameUtils.normalizeNoEndSeparator(home);

        String bundleDir = FilenameUtils.concat(home, "bundles");
        config.put(Constants.FRAMEWORK_STORAGE, bundleDir);

        //FIXME Pojo SR currently reads this from system property instead of Framework Property
        System.setProperty(Constants.FRAMEWORK_STORAGE, bundleDir);

        //Directory used by Felix File Install to watch for configs
        config.put("felix.fileinstall.dir", FilenameUtils.concat(home, "config"));

        //Set log level for config to INFO LogService.LOG_INFO
        config.put("felix.fileinstall.log.level", "3");

        //This ensures that configuration is registered in main thread
        //and not in a different thread
        config.put("felix.fileinstall.noInitialDelay", "true");

        config.put("repository.home", FilenameUtils.concat(home, "repository"));

        copyConfigToSystemProps(config);
    }

    @SuppressWarnings("unchecked")
    private static void copyConfigToSystemProps(Map config) {
        //TODO This is a temporary workaround as the current release version
        //of PojoSR reads value from System properties. Trunk version reads from
        //initial map. This should be removed when we move to version which has the fix
        Iterator<Map.Entry> itr = config.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry e = itr.next();
            if (e.getValue() instanceof String) {
                System.setProperty((String) e.getKey(), (String) e.getValue());
            }
        }
    }

    private PojoServiceRegistry createServiceRegistry(Map<String, Object> config) {
        try {
            ServiceLoader<PojoServiceRegistryFactory> loader = ServiceLoader.load(PojoServiceRegistryFactory.class);
            return loader.iterator().next().newPojoServiceRegistry(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private void startBundles(PojoServiceRegistry registry) {
        try {
            List<BundleDescriptor> descriptors = new ClasspathScanner().scanForBundles();
            descriptors = Lists.newArrayList(descriptors);
            descriptors = processDescriptors(descriptors);
            registry.startBundles(descriptors);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class RepositoryTracker extends ServiceTracker {
        private final SettableFuture<Repository> repoFuture;
        private final PojoServiceRegistry registry;
        private RepositoryProxy proxy;

        public RepositoryTracker(PojoServiceRegistry registry, SettableFuture<Repository> repoFuture) {
            super(registry.getBundleContext(), Repository.class.getName(), null);
            this.repoFuture = repoFuture;
            this.registry = registry;
            this.open();
        }

        @Override
        public Object addingService(ServiceReference reference) {
            Object service = super.addingService(reference);
            if (proxy == null) {
                //As its possible that future is accessed before the service
                //get registered with tracker. We also capture the initial reference
                //and use that for the first access case
                repoFuture.set(createProxy((Repository) service));
            }
            return service;
        }

        @Override
        public void removedService(ServiceReference reference, Object service) {
            if (proxy != null) {
                proxy.clearInitialReference();
            }
        }

        public PojoServiceRegistry getRegistry() {
            return registry;
        }

        private Repository createProxy(Repository service) {
            proxy = new RepositoryProxy(this, service);
            return (Repository) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class[]{Repository.class, JackrabbitRepository.class}, proxy);
        }
    }

    /**
     * Due to the way SecurityConfiguration is managed in OSGi env its possible
     * that repository gets created/shutdown few times. So need to have a proxy
     * to access the latest service
     */
    private static class RepositoryProxy implements InvocationHandler {
        private final RepositoryTracker tracker;
        private Repository initialService;

        private RepositoryProxy(RepositoryTracker tracker, Repository initialService) {
            this.tracker = tracker;
            this.initialService = initialService;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Object obj = tracker.getService();
            if (obj == null) {
                obj = initialService;
            }

            Preconditions.checkNotNull(obj, "Repository service is not available");

            if ("shutdown".equals(method.getName())) {
                shutdown(tracker.getRegistry());
            }

            return method.invoke(obj, args);
        }

        public void clearInitialReference() {
            this.initialService = null;
        }
    }
}
