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

package org.apache.jackrabbit.oak.run.osgi

import org.apache.felix.connect.launch.PojoServiceRegistry
import org.apache.commons.io.FilenameUtils
import org.apache.jackrabbit.api.JackrabbitRepository
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import org.osgi.framework.BundleContext
import org.osgi.framework.ServiceReference
import org.osgi.service.cm.ConfigurationAdmin
import org.osgi.util.tracker.ServiceTracker

import javax.jcr.*
import java.util.concurrent.TimeUnit

import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_HOME
import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_TIMEOUT_IN_SECS
import static org.junit.Assert.fail

abstract class AbstractRepositoryFactoryTest{
    static final int SVC_WAIT_TIME = Integer.getInteger("pojosr.waitTime", 10)
    Map config
    File workDir
    Repository repository
    RepositoryFactory repositoryFactory = new OakOSGiRepositoryFactory();

    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder(new File("target"))

    @Before
    void setUp() {
        workDir = tmpFolder.getRoot();
        config = [
                (REPOSITORY_HOME): workDir.absolutePath,
                (REPOSITORY_TIMEOUT_IN_SECS) : 60,
        ]
    }

    @After
    void tearDown() {
        try {
            if (repository == null) {
                PojoServiceRegistry registry = getRegistry()
                OakOSGiRepositoryFactory.shutdown(registry, 5)
            }
        } catch (AssertionError ignore){

        }

        if (repository instanceof JackrabbitRepository) {
            ((JackrabbitRepository) repository).shutdown();
        }
    }

    protected PojoServiceRegistry getRegistry() {
        assert repository instanceof ServiceRegistryProvider
        return ((ServiceRegistryProvider) repository).getServiceRegistry()
    }

    protected <T> void assertNoService(Class<T> clazz) {
        ServiceReference<T> sr = registry.getServiceReference(clazz.name)
        assert sr == null: "Service of type $clazz was found"
    }

    protected <T> T getService(Class<T> clazz) {
        ServiceReference<T> sr = registry.getServiceReference(clazz.name)
        assert sr: "Not able to found a service of type $clazz"
        return (T) registry.getService(sr)
    }

    protected <T> T getServiceWithWait(Class<T> clazz) {
        return getServiceWithWait(clazz, getRegistry().bundleContext)
    }

    protected static <T> T getServiceWithWait(Class<T> clazz, BundleContext bundleContext) {
        ServiceTracker st = new ServiceTracker(bundleContext, clazz.name, null)
        st.open()
        T sr = (T) st.waitForService(TimeUnit.SECONDS.toMillis(SVC_WAIT_TIME))
        assert sr , "No service found for ${clazz.name}"
        return sr
    }

    protected File getResource(String path){
        File file = new File(FilenameUtils.concat(getBaseDir(), "src/test/resources/$path"))
        assert file.exists() : "No file found at ${file.absolutePath}"
        return file
    }

    protected void createConfig(Map config){
        ConfigurationAdmin cm = getService(ConfigurationAdmin.class)
        ConfigInstaller ci = new ConfigInstaller(cm, getRegistry().bundleContext)
        ci.installConfigs(config)
    }

    protected Session createAdminSession() throws RepositoryException {
        return getRepository().login(new SimpleCredentials("admin", "admin".toCharArray()));
    }

    protected String createConfigValue(String ... configFiles){
        return configFiles.collect {getResource(it).absolutePath}.join(',')
    }

    private static String getBaseDir() {
        // 'basedir' is set by Maven Surefire. It always points to the current subproject,
        // even in reactor builds.
        String baseDir = System.getProperty("basedir");
        if(baseDir) {
            return baseDir
        }
        return new File(".").getAbsolutePath();
    }

    static retry(int timeoutSeconds, int intervalBetweenTriesMsec, Closure c) {
        long timeout = System.currentTimeMillis() + timeoutSeconds * 1000L;
        while (System.currentTimeMillis() < timeout) {
            try {
                if (c.call()) {
                    return;
                }
            } catch (AssertionError ignore) {
            } catch (Exception ignore) {
            }

            try {
                Thread.sleep(intervalBetweenTriesMsec);
            } catch (InterruptedException ignore) {
            }
        }

        fail("RetryLoop failed, condition is false after " + timeoutSeconds + " seconds: ");
    }

}
