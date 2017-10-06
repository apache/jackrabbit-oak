/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.upgrade;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.util.VersionCopyTestUtils.VersionCopySetup;
import org.junit.Before;

import javax.jcr.RepositoryException;
import javax.jcr.Session;

import java.io.IOException;

public class CopyVersionHistorySidegradeTest extends CopyVersionHistoryTest {

    private static NodeStore sourceNodeStore;

    @Before
    @Override
    public void upgradeRepository() throws Exception {
        if (sourceNodeStore == null) {
            sourceNodeStore = new MemoryNodeStore();
            RepositoryImpl repository = (RepositoryImpl) new Jcr(new Oak(sourceNodeStore)).createRepository();
            Session session = repository.login(CREDENTIALS);
            try {
                createSourceContent(session);
            } finally {
                session.logout();
                repository.shutdown();
            }
        }
    }

    @Override
    protected void migrate(VersionCopySetup setup, NodeStore target, String includePath) throws RepositoryException, IOException {
        final RepositorySidegrade sidegrade = new RepositorySidegrade(sourceNodeStore, target);
        sidegrade.setIncludes(includePath);
        setup.setup(sidegrade.versionCopyConfiguration);
        sidegrade.copy();
    }
}