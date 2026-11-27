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

import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.core.RepositoryContext;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.util.VersionCopyTestUtils.VersionCopySetup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Exercises version history copying from a Classic repository via {@link RepositoryUpgrade}.
 */
public class CopyVersionHistoryTest extends AbstractCopyVersionHistoryTest {

    private static final String VERSIONABLE_PATH = "/versionables/mix:versionable/young";

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    private File source;

    /** Creates a Classic source with the histories needed by the tests. */
    @Before
    public void createSourceRepository() throws Exception {
        source = folder.newFolder();
        RepositoryContext context = RepositoryContext.create(RepositoryConfig.install(source));
        try {
            Session session = context.getRepository().login(CREDENTIALS);
            try {
                createSourceContent(session);
            } finally {
                session.logout();
            }
        } finally {
            context.getRepository().shutdown();
        }
    }

    /** Verifies that early shutdown happens after content is copied but before custom hooks run. */
    @Test
    public void shutsDownSourceBeforeCustomHooks() throws RepositoryException, IOException {
        NodeStore target = SegmentNodeStoreBuilders.builder(new MemoryStore()).build();
        RepositoryContext context = RepositoryContext.create(RepositoryConfig.create(source));
        try {
            RepositoryUpgrade upgrade = new RepositoryUpgrade(context, target);
            upgrade.setEarlyShutdown(true);
            upgrade.setCopyVersions(null);
            upgrade.setCustomCommitHooks(List.of((before, after, info) -> {
                Assert.assertTrue(after.hasChildNode("versionables"));
                Assert.assertThrows(RepositoryException.class, () -> context.getRepository().login(CREDENTIALS).logout());
                return after;
            }));
            upgrade.copy(null);
        } finally {
            context.getRepository().shutdown();
        }
    }

    /** Verifies that copying referenced histories without orphaned histories defers shutdown. */
    @Test
    public void keepsSourceOpenForSelectiveVersionCopy() throws RepositoryException {
        RepositoryContext context = RepositoryContext.create(RepositoryConfig.create(source));
        NodeStore target = new MemoryNodeStore();
        try {
            RepositoryUpgrade upgrade = new RepositoryUpgrade(context, target);
            upgrade.setEarlyShutdown(true);
            upgrade.setCopyOrphanedVersions(null);
            upgrade.setCustomCommitHooks(List.of((before, after, info) -> {
                try {
                    context.getRepository().login(CREDENTIALS).logout();
                } catch (RepositoryException e) {
                    throw new AssertionError("Source must remain open during selective version copy", e);
                }
                return after;
            }));
            upgrade.copy(null);
        } finally {
            context.getRepository().shutdown();
        }

        RepositoryImpl repository = (RepositoryImpl) new Jcr(new Oak(target)).createRepository();
        try {
            Session session = repository.login(CREDENTIALS);
            try {
                Assert.assertEquals("1.2", session.getWorkspace().getVersionManager()
                        .getBaseVersion(VERSIONABLE_PATH).getName());
            } finally {
                session.logout();
            }
        } finally {
            repository.shutdown();
        }
    }

    @Override
    protected void migrate(VersionCopySetup setup, NodeStore target, String includePath)
            throws RepositoryException, IOException {
        final RepositoryContext sourceContext = RepositoryContext.create(RepositoryConfig.create(source));
        try {
            final RepositoryUpgrade upgrade = new RepositoryUpgrade(sourceContext, target);
            upgrade.setIncludes(includePath);
            setup.setup(upgrade.versionCopyConfiguration);
            upgrade.setEarlyShutdown(false);
            upgrade.copy(null);
        } finally {
            sourceContext.getRepository().shutdown();
        }
    }
}
