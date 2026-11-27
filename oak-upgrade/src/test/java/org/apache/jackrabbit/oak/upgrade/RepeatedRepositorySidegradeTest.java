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

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;

import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class RepeatedRepositorySidegradeTest extends AbstractRepeatedRepositoryMigrationTest {

    @Override
    protected void copyTwice(File source, NodeStore target, FileStore targetStore) throws Exception {
        updateSource(source, this::createSourceContent);
        sidegrade(source, target);

        updateSource(source, this::modifySourceContent);
        sidegrade(source, target);
    }

    private void updateSource(File source, SourceUpdate update) throws Exception {
        try (FileStore fileStore = fileStoreBuilder(source).build()) {
            SegmentNodeStore sourceStore = SegmentNodeStoreBuilders.builder(fileStore).build();
            RepositoryImpl repository = (RepositoryImpl) new Jcr(new Oak(sourceStore)).createRepository();
            try {
                Session session = repository.login(CREDENTIALS);
                try {
                    update.apply(session);
                } finally {
                    session.logout();
                }
            } finally {
                repository.shutdown();
            }
        }
    }

    private void sidegrade(File source, NodeStore target) throws Exception {
        try (FileStore fileStore = fileStoreBuilder(source).build()) {
            SegmentNodeStore sourceStore = SegmentNodeStoreBuilders.builder(fileStore).build();
            NodeBuilder builder = target.getRoot().builder();
            builder.child("foo").child("bar");
            target.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            new RepositorySidegrade(sourceStore, target).copy();
        } catch (CommitFailedException e) {
            throw new RepositoryException(e);
        }
    }
}
