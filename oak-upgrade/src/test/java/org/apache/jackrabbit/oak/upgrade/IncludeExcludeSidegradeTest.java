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

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;
import java.io.IOException;

import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;

public class IncludeExcludeSidegradeTest extends IncludeExcludeUpgradeTest {

    @Before
    public synchronized void upgradeRepository() throws Exception {
        if (targetNodeStore == null) {
            File directory = getTestDirectory();
            File source = new File(directory, "source");
            source.mkdirs();
            FileStore fileStore = fileStoreBuilder(source).build();
            SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
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
            doUpgradeRepository(source, target);
            targetNodeStore = target;
        }
    }

    @Override
    protected void doUpgradeRepository(File source, NodeStore target) throws RepositoryException, IOException {
        FileStore fileStore;
        try {
            fileStore = fileStoreBuilder(source).build();
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }
        SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        try {
            final RepositorySidegrade sidegrade = new RepositorySidegrade(segmentNodeStore, target);
            sidegrade.setIncludes(
                    "/content/foo/en",
                    "/content/assets/foo",
                    "/content/other"
            );
            sidegrade.setExcludes(
                    "/content/assets/foo/2013",
                    "/content/assets/foo/2012",
                    "/content/assets/foo/2011",
                    "/content/assets/foo/2010"
            );
            sidegrade.copy();
        } finally {
            fileStore.close();
        }
    }
}
