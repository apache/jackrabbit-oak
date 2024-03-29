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

import java.io.File;
import java.util.Properties;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;

/**
 * A repository stub implementation for Oak Segment Tar
 */
public class OakSegmentTarRepositoryStub extends BaseRepositoryStub {

    private final FileStore store;

    private final Repository repository;

    /**
     * Constructor as required by the JCR TCK.
     *
     * @param settings repository settings
     * @throws RepositoryException If an error occurs.
     */
    public OakSegmentTarRepositoryStub(Properties settings) throws RepositoryException {
        super(settings);

        try {
            File directory = new File("target", "segment-tar-" + System.currentTimeMillis());
            this.store = FileStoreBuilder.fileStoreBuilder(directory).withMaxFileSize(1).build();
            Oak oak = new Oak(SegmentNodeStoreBuilders.builder(store).build());
            Jcr jcr = new Jcr(oak);
            preCreateRepository(jcr, oak.getWhiteboard());
            this.repository = jcr.createRepository();
            loadTestContent(repository);
        } catch (Exception e) {
            throw new RepositoryException(e);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                store.close();
            }
        }));
    }

    /**
     * Override in subclass and perform additional configuration on the
     * {@link Jcr} builder before the repository is created. This default
     * implementation set query engine settings as returned by
     * {@link #getQueryEngineSettings()} and adds a
     * {@link org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigInitializer}.
     *
     * @param jcr the builder.
     * @param whiteboard the oak whiteboard
     */
    protected void preCreateRepository(Jcr jcr, Whiteboard whiteboard) {
        this.preCreateRepository(jcr);
    }

    /**
     * Returns the configured repository instance.
     *
     * @return the configured repository instance.
     */
    @Override
    public synchronized Repository getRepository() {
        return repository;
    }
}
