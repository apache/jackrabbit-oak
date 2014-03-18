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
package org.apache.jackrabbit.oak.upgrade;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;

import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.core.RepositoryImpl;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Before;
import org.junit.BeforeClass;

public abstract class AbstractRepositoryUpgradeTest {

    protected static final Credentials CREDENTIALS = new SimpleCredentials("admin", "admin".toCharArray());

    private static Repository targetRepository;

    @BeforeClass
    public static void init() {
        // ensure that we create a new repository for the next test
        targetRepository = null;
    }

    @Before
    public synchronized void upgradeRepository() throws Exception {
        if (targetRepository == null) {
            File directory = new File(
                    "target", "upgrade-" + Clock.SIMPLE.getTimeIncreasing());
            FileUtils.deleteQuietly(directory);

            File source = new File(directory, "source");
            source.mkdirs();

            InputStream repoConfig = getRepositoryConfig();
            RepositoryConfig config;
            if (repoConfig == null) {
                config = RepositoryConfig.install(source);
            } else {
                OutputStream out = FileUtils.openOutputStream(new File(source, "repository.xml"));
                IOUtils.copy(repoConfig, out);
                out.close();
                repoConfig.close();
                config = RepositoryConfig.create(source);
            }
            RepositoryImpl repository = RepositoryImpl.create(config);
            try {
                createSourceContent(repository);
            } finally {
                repository.shutdown();
            }
            NodeStore target = new KernelNodeStore(new MicroKernelImpl());
            RepositoryUpgrade.copy(source, target);
            targetRepository = new Jcr(new Oak(target)).createRepository();
        }
    }

    public InputStream getRepositoryConfig() {
        return null;
    }

    public Repository getTargetRepository() {
        return targetRepository;
    }

    public JackrabbitSession createAdminSession() throws RepositoryException {
        return (JackrabbitSession) getTargetRepository().login(CREDENTIALS);
    }

    protected abstract void createSourceContent(Repository repository) throws Exception;

}
