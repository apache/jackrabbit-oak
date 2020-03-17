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

package org.apache.jackrabbit.oak.index;

import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.run.cli.DocumentBuilderCustomizer;
import org.apache.jackrabbit.oak.run.cli.DocumentNodeStoreOptions;
import org.apache.jackrabbit.oak.run.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.io.FileUtils.ONE_GB;

class IndexDocumentBuilderCustomizer implements DocumentBuilderCustomizer {
    private static final String PERSISTENT_CACHE_PROP = "oak.documentMK.persCache";
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Options opts;
    private final DocumentNodeStoreOptions docStoreOpts;
    private final boolean readOnlyAccess;

    IndexDocumentBuilderCustomizer(Options opts, boolean readOnlyAccess) {
        this.opts = opts;
        docStoreOpts = opts.getOptionBean(DocumentNodeStoreOptions.class);
        this.readOnlyAccess = readOnlyAccess;
    }

    @Override
    public void customize(DocumentNodeStoreBuilder<?> builder) throws IOException {
        configurePersistentCache(builder);
        configureCacheSize(builder);

        if (readOnlyAccess) {
            configureCacheForReadOnlyMode(builder);
        }
    }

    private void configurePersistentCache(DocumentNodeStoreBuilder<?> builder) throws IOException {
        if (System.getProperty(PERSISTENT_CACHE_PROP) == null) {
            File temp = opts.getOptionBean(IndexOptions.class).getWorkDir();
            File cache = new File(temp, "cache");
            String cacheConfig = String.format("%s,size=4096,binary=0,-nodes,-children", cache.getAbsolutePath());
            builder.setPersistentCache(cacheConfig);
            log.info("Persistent cache set to [{}]", cacheConfig);
        }
    }

    private void configureCacheForReadOnlyMode(DocumentNodeStoreBuilder<?> builder) {
        if (!docStoreOpts.isCacheDistributionDefined()) {
            builder.memoryCacheDistribution(
                    35,
                    10,
                    15,
                    2
            );
        }

        // usage of this DocumentNodeStore is single threaded. Reduce the
        // number of cache segments to a minimum. This allows for caching
        // bigger entries that would otherwise be evicted immediately
        //TODO Should not be done if later we implement multithreaded indexing
        builder.setCacheSegmentCount(1);
        log.info("Configuring cache for single threaded access");
    }

    private void configureCacheSize(DocumentNodeStoreBuilder<?> builder) {
        //Set cache size to max 4GB or half of min memory
        if (docStoreOpts.getCacheSize() == 0) {
            long maxMem = Runtime.getRuntime().maxMemory();
            long memToUse = Math.min(ONE_GB * 4, maxMem / 2);
            builder.memoryCacheSize(memToUse);
            log.info("Initializing cache size to {} ({})", memToUse, IOUtils.humanReadableByteCount(memToUse));
        }
    }
}
