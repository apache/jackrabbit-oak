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

import de.kalpatec.pojosr.framework.launch.PojoServiceRegistry
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
import org.apache.jackrabbit.api.JackrabbitRepository
import org.junit.After
import org.junit.Before
import org.osgi.framework.ServiceReference
import org.osgi.service.cm.ConfigurationAdmin
import org.osgi.util.tracker.ServiceTracker

import javax.jcr.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_HOME
import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_STARTUP_TIMEOUT

abstract class AbstractRepositoryFactoryTest{
    static final int SVC_WAIT_TIME = Integer.getInteger("pojosr.waitTime", 10)
    static final AtomicInteger counter = new AtomicInteger()
    Map config
    File workDir
    Repository repository
    RepositoryFactory repositoryFactory = new OakOSGiRepositoryFactory();

    @Before
    void setUp() {
        workDir = new File("target", "repotest-${counter.incrementAndGet()}-${System.currentTimeMillis()}");
        config = [
                (REPOSITORY_HOME): workDir.absolutePath,
                (REPOSITORY_STARTUP_TIMEOUT) : 2
        ]
    }

    @After
    void tearDown() {
        if (repository instanceof JackrabbitRepository) {
            ((JackrabbitRepository) repository).shutdown();
        }

        if (workDir.exists()) {
            FileUtils.deleteQuietly(workDir);
        }
    }

    protected PojoServiceRegistry getRegistry() {
        assert repository instanceof ServiceRegistryProvider
        return ((ServiceRegistryProvider) repository).getServiceRegistry()
    }

    protected <T> T getService(Class<T> clazz) {
        ServiceReference<T> sr = registry.getServiceReference(clazz.name)
        assert sr: "Not able to found a service of type $clazz"
        return (T) registry.getService(sr)
    }

    protected <T> T getServiceWithWait(Class<T> clazz) {
        ServiceTracker st = new ServiceTracker(getRegistry().bundleContext, clazz.name, null)
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

    private static String getBaseDir() {
        // 'basedir' is set by Maven Surefire. It always points to the current subproject,
        // even in reactor builds.
        String baseDir = System.getProperty("basedir");
        if(baseDir) {
            return baseDir
        }
        return new File(".").getAbsolutePath();
    }

}
