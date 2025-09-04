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
package org.apache.jackrabbit.oak.jcr;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.jcr.Repository;
import javax.jcr.Session;
import java.io.File;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertNotNull;

public class ReadOnlyJcrTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Test
    public void createJcrOnReadOnlyNodeStore() throws Exception {
        try (FileStore store = newFileStoreBuilder().build()) {
            SegmentNodeStore ns = SegmentNodeStoreBuilders.builder(store).build();
            // create and close immediately
            close(new Jcr(ns).createRepository());
        }
        // now open read-only
        try (ReadOnlyFileStore store = newFileStoreBuilder().buildReadOnly()) {
            SegmentNodeStore ns = SegmentNodeStoreBuilders.builder(store).build();
            Jcr jcr = new Jcr(new Oak(ns), false);
            jcr.with(new OpenSecurityProvider());
            Repository repo = jcr.createRepository();
            Session s = repo.login();
            try {
                // do some basic read operation
                assertNotNull(s.getRootNode().getPrimaryNodeType().getName());
            } finally {
                s.logout();
            }
            close(repo);
        }
    }

    private FileStoreBuilder newFileStoreBuilder() {
        return fileStoreBuilder(folder.getRoot());
    }

    private static void close(Repository repository) {
        if (repository instanceof RepositoryImpl) {
            ((RepositoryImpl) repository).shutdown();
        } else {
            throw new IllegalArgumentException("Repository is not an instance of RepositoryImpl");
        }
    }
}
