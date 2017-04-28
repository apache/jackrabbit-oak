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

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.io.File;
import java.io.IOException;

public class RepeatedRepositorySidegradeTest extends RepeatedRepositoryUpgradeTest {

    @Before
    public synchronized void upgradeRepository() throws Exception {
        if (!upgradeComplete) {
            final File sourceDir = new File(getTestDirectory(), "jackrabbit2");

            sourceDir.mkdirs();

            FileStore fileStore = FileStore.newFileStore(sourceDir).create();
            SegmentNodeStore segmentNodeStore = new SegmentNodeStore(fileStore);
            RepositoryImpl repository = (RepositoryImpl) new Jcr(new Oak(segmentNodeStore)).createRepository();
            Session session = repository.login(CREDENTIALS);
            try {
                createSourceContent(session);
            } finally {
                session.save();
                session.logout();
                repository.shutdown();
                fileStore.close();
            }

            final NodeStore target = getTargetNodeStore();
            doUpgradeRepository(sourceDir, target, false);
            fileStore.flush();

            fileStore = FileStore.newFileStore(sourceDir).create();
            segmentNodeStore = new SegmentNodeStore(fileStore);
            repository = (RepositoryImpl) new Jcr(new Oak(segmentNodeStore)).createRepository();
            session = repository.login(CREDENTIALS);
            try {
                modifySourceContent(session);
            } finally {
                session.save();
                session.logout();
                repository.shutdown();
                fileStore.close();
            }

            doUpgradeRepository(sourceDir, target, true);
            fileStore.flush();

            upgradeComplete = true;
        }
    }

    @Override
    protected void doUpgradeRepository(File source, NodeStore target, boolean skipInit) throws RepositoryException, IOException {
        FileStore fileStore = FileStore.newFileStore(source).create();
        SegmentNodeStore segmentNodeStore = new SegmentNodeStore(fileStore);
        try {
            final RepositorySidegrade repositoryUpgrade = new RepositorySidegrade(segmentNodeStore, target);

            NodeBuilder builder = target.getRoot().builder();
            builder.child("foo").child("bar");
            target.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            repositoryUpgrade.copy();
        } catch (CommitFailedException e) {
            throw new RepositoryException(e);
        } finally {
            fileStore.close();
        }
    }
}
