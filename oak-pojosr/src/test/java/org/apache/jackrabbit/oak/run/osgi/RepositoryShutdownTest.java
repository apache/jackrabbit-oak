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

import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_HOME;
import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_TIMEOUT_IN_SECS;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.jcr.Repository;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

public class RepositoryShutdownTest {

    @Test
    public void multipleShutdown() throws Exception {
        JackrabbitRepository repository = Mockito.mock(JackrabbitRepository.class);
        Map<String, Object> config = getConfig(repository);

        Repository repo2 = new OakOSGiRepositoryFactory().getRepository(config);
        assertTrue(repo2 instanceof JackrabbitRepository);
        ((JackrabbitRepository) repo2).shutdown();

        Mockito.verify(repository, Mockito.times(0)).shutdown();
    }

    @Test
    public void multipleShutdownAndWait() throws Exception {
        JackrabbitRepository repository = Mockito.mock(JackrabbitRepository.class);
        Map<String, Object> config = getConfig(repository);

        Repository repo2 = new OakOSGiRepositoryFactory().getRepository(config);
        assertTrue(repo2 instanceof JackrabbitRepository);
        ((JackrabbitRepository) repo2).shutdown();
        ((JackrabbitRepository) repo2).shutdown();
    }

    private Map<String, Object> getConfig(final JackrabbitRepository repository) {
        Map<String, Object> map = new LinkedHashMap<>(3);
        map.put(REPOSITORY_HOME, tmpFolder.getRoot().getAbsolutePath());
        map.put(REPOSITORY_TIMEOUT_IN_SECS, 1);
        map.put(BundleActivator.class.getName(), new BundleActivator() {
            @Override
            public void start(BundleContext bundleContext) throws Exception {
                bundleContext.registerService(Repository.class.getName(), repository, null);
            }

            @Override
            public void stop(BundleContext bundleContext) throws Exception {

            }

        });
        return map;
    }

    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder(new File("target"));
}
