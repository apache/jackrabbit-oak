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

import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.core.RepositoryContext;
import org.apache.jackrabbit.core.RepositoryImpl;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * Exercises incremental copying from Classic with {@link RepositoryUpgrade}.
 */
public class RepeatedRepositoryUpgradeTest extends AbstractRepeatedRepositoryMigrationTest {

    @Override
    protected void copyTwice(File source, NodeStore target, FileStore targetStore) throws Exception {
        updateSource(RepositoryConfig.install(source), this::createSourceContent);
        upgrade(source, target, false);
        targetStore.flush();

        updateSource(RepositoryConfig.create(source), this::modifySourceContent);
        upgrade(source, target, true);
        targetStore.flush();
    }

    private void updateSource(RepositoryConfig config, SourceUpdate update) throws RepositoryException {
        RepositoryImpl repository = RepositoryImpl.create(config);
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

    private void upgrade(File source, NodeStore target, boolean skipInitialization)
            throws RepositoryException, IOException {
        RepositoryContext context = RepositoryContext.create(RepositoryConfig.create(source));
        try {
            RepositoryUpgrade upgrade = new RepositoryUpgrade(context, target);
            upgrade.setSkipInitialization(skipInitialization);
            upgrade.copy(builder -> builder.child("foo").child("bar"));
        } finally {
            context.getRepository().shutdown();
        }
    }
}
