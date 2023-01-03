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


import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexerInfo;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.util.Properties;

public class ElasticIndexerSupport extends IndexerSupport {

    public ElasticIndexerSupport(IndexHelper indexHelper, String checkpoint) {
        super(indexHelper, checkpoint);
    }

    @Override
    public void postIndexWork(NodeStore copyOnWriteStore) throws CommitFailedException, IOException {
        switchIndexLanesBack(copyOnWriteStore);
        dumpIndexDefinitions(copyOnWriteStore);
        createIndexMetaDataForElastic();
    }

    private void createIndexMetaDataForElastic() throws IOException {
        // Need to create this to make have similar meta data file as created by lucene
        // Effectively this will help us reuse the IndexImporter class in a better way without having to rewrite it completely
        // for elastic and help avoid code duplication
        for (String indexPath : indexHelper.getIndexPaths()) {
            File dir = new File(getLocalIndexDir(), indexPath.substring(indexPath.lastIndexOf("/")));
            FileUtils.forceMkdir(dir);
            File infoFile = new File(dir, IndexerInfo.INDEX_METADATA_FILE_NAME);
            infoFile.createNewFile();
            Properties p = new Properties();
            p.setProperty("indexPath", indexPath);
            try (OutputStream os = new BufferedOutputStream(new FileOutputStream(infoFile))) {
                p.store(os, "Indexer info");
            }
        }
    }
}
