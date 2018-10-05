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

import org.apache.jackrabbit.api.JackrabbitRepository
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.osgi.framework.BundleActivator
import org.osgi.framework.BundleContext

import javax.jcr.Repository

import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_HOME
import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_TIMEOUT_IN_SECS
import static org.mockito.Mockito.mock
import static org.mockito.Mockito.times
import static org.mockito.Mockito.verify


class RepositoryShutdownTest {

    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder(new File("target"))

    @Test
    public void multipleShutdown() throws Exception{
        JackrabbitRepository repository = mock(JackrabbitRepository.class)
        def config = getConfig(repository)

        Repository repo2 = new OakOSGiRepositoryFactory().getRepository(config)
        assert repo2 instanceof JackrabbitRepository
        repo2.shutdown()

        verify(repository, times(0)).shutdown()
    }

    @Test
    public void multipleShutdownAndWait() throws Exception{
        JackrabbitRepository repository = mock(JackrabbitRepository.class)
        def config = getConfig(repository)

        Repository repo2 = new OakOSGiRepositoryFactory().getRepository(config)
        assert repo2 instanceof JackrabbitRepository
        repo2.shutdown()
        repo2.shutdown()
    }

    private LinkedHashMap<String, Object> getConfig(repository) {
        def config = [
                (REPOSITORY_HOME)           : tmpFolder.root.absolutePath,
                (REPOSITORY_TIMEOUT_IN_SECS): 1,
                (BundleActivator.class.name): new BundleActivator() {
                    @Override
                    void start(BundleContext bundleContext) throws Exception {
                        bundleContext.registerService(Repository.class.name, repository, null)
                    }

                    @Override
                    void stop(BundleContext bundleContext) throws Exception {

                    }
                }
        ]
        config
    }
}
