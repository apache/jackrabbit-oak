/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.jackrabbit.oak.run.osgi;

import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_HOME;
import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_TIMEOUT_IN_SECS;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import org.apache.commons.io.FilenameUtils;
import org.apache.felix.connect.launch.PojoServiceRegistry;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.osgi.framework.ServiceReference;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.runtime.ServiceComponentRuntime;
import org.osgi.service.component.runtime.dto.ComponentDescriptionDTO;
import org.osgi.util.promise.Promise;
import org.osgi.util.tracker.ServiceTracker;

public abstract class AbstractRepositoryFactoryTest {

    static final int SVC_WAIT_TIME = Integer.getInteger("pojosr.waitTime", 100);
    Map<String, Object> config;
    File workDir;
    Repository repository;
    OakOSGiRepositoryFactory repositoryFactory = new OakOSGiRepositoryFactory();

    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder(new File("target"));

    @Before
    public void setUp() {
        workDir = tmpFolder.getRoot();
        config = new HashMap<>();
        config.put(REPOSITORY_HOME, workDir.getAbsolutePath());
        config.put(REPOSITORY_TIMEOUT_IN_SECS, 60);
    }

    @After
    public void tearDown() {
        try {
            if (repository == null) {
                PojoServiceRegistry registry = getRegistry();
                OakOSGiRepositoryFactory.shutdown(registry, 5);
            }
        } catch (AssertionError ignore) {
        } catch (BundleException bundleException) {
            throw new RuntimeException(bundleException);
        }

        if (repository instanceof JackrabbitRepository) {
            ((JackrabbitRepository) repository).shutdown();
        }
    }

    protected PojoServiceRegistry getRegistry() {
        assertTrue(repository instanceof ServiceRegistryProvider);
        return ((ServiceRegistryProvider) repository).getServiceRegistry();
    }

    protected <T> void assertNoService(Class<T> clazz) {
        ServiceReference<T> sr = (ServiceReference<T>) getRegistry().getServiceReference(clazz.getName());
        assertNull("Service of type " + clazz + " was found", sr);
    }

    protected <T> T getService(Class<T> clazz) {
        ServiceReference<T> sr = (ServiceReference<T>) getRegistry().getServiceReference(clazz.getName());
        assertNotNull("Not able to find a service of type " + clazz, sr);
        return getRegistry().getService(sr);
    }

    protected <T> T getServiceWithWait(Class<T> clazz) {
        return getServiceWithWait(clazz, getRegistry().getBundleContext());
    }

    protected static <T> T getServiceWithWait(Class<T> clazz, BundleContext bundleContext) {
        try {
            ServiceTracker<T, T> st = new ServiceTracker<>(bundleContext, clazz.getName(), null);
            st.open();
            T sr = st.waitForService(TimeUnit.SECONDS.toMillis(SVC_WAIT_TIME));
            assertNotNull("No service found for " + clazz.getName(), sr);
            return sr;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected File getResource(String path) {
        File file = new File(FilenameUtils.concat(getBaseDir(), "src/test/resources/" + path));
        assertTrue("No file found at " + file.getAbsolutePath(), file.exists());
        return file;
    }

    protected void createConfig(Map<String, Map<String, Object>> config) {
        try {
            ConfigurationAdmin cm = getService(ConfigurationAdmin.class);
            ConfigInstaller ci = new ConfigInstaller(cm, getRegistry().getBundleContext());
            ci.installConfigs(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected Session createAdminSession() throws RepositoryException {
        return repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
    }

    protected String createConfigValue(String... configFiles) {
        StringBuilder sb = new StringBuilder();
        for (String configFile : configFiles) {
            sb.append(getResource(configFile).getAbsolutePath()).append(',');
        }
        return sb.substring(0, sb.length() - 1);
    }

    private static String getBaseDir() {
        String baseDir = System.getProperty("basedir");
        if (baseDir != null) {
            return baseDir;
        }
        return new File(".").getAbsolutePath();
    }

    protected static void retry(int timeoutSeconds, int intervalBetweenTriesMsec, Runnable c) {
        retry(timeoutSeconds, intervalBetweenTriesMsec, null, c);
    }

    protected static void retry(int timeoutSeconds, int intervalBetweenTriesMsec, String message, Runnable c) {
        long timeout = System.currentTimeMillis() + timeoutSeconds * 1000L;
        while (System.currentTimeMillis() < timeout) {
            try {
                c.run();
                return;
            } catch (AssertionError | Exception ignore) {
            }

            try {
                Thread.sleep(intervalBetweenTriesMsec);
            } catch (InterruptedException ignore) {
            }
        }

        fail("RetryLoop failed, condition is false after " + timeoutSeconds + " seconds" + (message != null ? ":"
                + message : ""));
    }

    protected static String classNameFilter(String className) {
        return "(|(objectClass=" + className + ")(service.pid=" + className + ")(service.factoryPid=" + className
                + "))";
    }

    protected void awaitServiceEvent(Runnable closure, String serviceFilter, int eventTypes, long timeout,
            TimeUnit timeUnit) {
        try {
            PojoServiceRegistry registry = getRegistry();
            BundleContext bundleContext = registry.getBundleContext();
            EventServiceListener listener = new EventServiceListener(eventTypes, bundleContext, serviceFilter);

            bundleContext.addServiceListener(listener);
            closure.run();
            if (!listener.getLatch().await(timeout, timeUnit)) {
                throw new AssertionError("Exceeded timeout waiting for service event matching " +
                        "[eventTypes: " + eventTypes + ", filter: " + serviceFilter + "], " +
                        "got " + listener.events.size() + " non matching events: [" + listener.events + "]");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void awaitServiceEvent(Runnable closure, String serviceFilter, int eventTypes) {
        long timeout = 1000;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        this.awaitServiceEvent(closure, serviceFilter, eventTypes, timeout, timeUnit);
    }

    protected void disableComponent(String name) {
        ServiceComponentRuntime scr = getServiceWithWait(ServiceComponentRuntime.class);
        ComponentDescriptionDTO dto = getComponentDTO(scr, name);
        Promise<Void> p = scr.disableComponent(dto);
        try {
            p.getValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void enableComponent(String name) {
        ServiceComponentRuntime scr = getServiceWithWait(ServiceComponentRuntime.class);
        ComponentDescriptionDTO dto = getComponentDTO(scr, name);
        Promise<Void> p = scr.enableComponent(dto);
        try {
            p.getValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ComponentDescriptionDTO getComponentDTO(ServiceComponentRuntime scr, String name) {
        return scr.getComponentDescriptionDTOs().stream()
                .filter(d -> d.name.equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No component found with name " + name));
    }

    private static class EventServiceListener implements ServiceListener {

        private final CountDownLatch latch;
        private final Map<String, Object> events;
        private final int eventTypes;
        private final BundleContext bundleContext;
        private final String serviceFilter;

        public EventServiceListener(int eventTypes, BundleContext bundleContext, String serviceFilter) {
            this.eventTypes = eventTypes;
            this.bundleContext = bundleContext;
            this.serviceFilter = serviceFilter;
            latch = new CountDownLatch(1);
            events = new HashMap<>();
        }

        @Override
        public void serviceChanged(ServiceEvent event) {
            events.put("eventType", event.getType());
            events.put("serviceProperties", asMap(event.getServiceReference()));
            try {
                if ((eventTypes & event.getType()) > 0 && bundleContext.createFilter(serviceFilter)
                        .match(event.getServiceReference())) {
                    latch.countDown();
                }
            } catch (InvalidSyntaxException e) {
                throw new RuntimeException(e);
            }
        }

        private Map<String, Object> asMap(ServiceReference<?> serviceReference) {
            Map<String, Object> map = new HashMap<>();
            for (String key : serviceReference.getPropertyKeys()) {
                map.put(key, serviceReference.getProperty(key));
            }
            return map;
        }

        public CountDownLatch getLatch() {
            return latch;
        }
    }
}