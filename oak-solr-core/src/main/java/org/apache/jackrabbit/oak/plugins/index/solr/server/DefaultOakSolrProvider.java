/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.solr.server;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.jackrabbit.mk.util.IOUtils;
import org.apache.jackrabbit.oak.plugins.index.solr.OakSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.SolrServerProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;

/**
 * Default implementation of {@link SolrServerProvider} and {@link OakSolrConfigurationProvider}
 * which hides an {@link EmbeddedSolrServer} configured as per passed {@link NodeState}
 * properties.
 */
public class DefaultOakSolrProvider implements SolrServerProvider, OakSolrConfigurationProvider {

    private final OakSolrNodeStateConfiguration oakSolrConfiguration;

    public DefaultOakSolrProvider(NodeState configurationNodeState) {
        this.oakSolrConfiguration = new FixedNodeStateConfiguration(configurationNodeState);
    }

    public DefaultOakSolrProvider(NodeStore store, String path) {
        this.oakSolrConfiguration = new UpToDateNodeStateConfiguration(store, path);
    }

    private SolrServer solrServer;

    private SolrServer createSolrServer() throws Exception {

        String solrHomePath = oakSolrConfiguration.getSolrHomePath();
        String coreName = oakSolrConfiguration.getCoreName();
        String solrConfigPath = oakSolrConfiguration.getSolrConfigPath();

        if (solrConfigPath != null && solrHomePath != null && coreName != null) {

            checkSolrConfiguration(solrHomePath, solrConfigPath, coreName);

            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(CoreContainer.class.getClassLoader());

            CoreContainer coreContainer = new CoreContainer(solrHomePath);
            try {
                coreContainer.load(solrHomePath, new File(solrConfigPath));
            } finally {
                Thread.currentThread().setContextClassLoader(classLoader);
            }

            return new EmbeddedSolrServer(coreContainer, coreName);
        }
        else {
            throw new Exception("SolrServer configuration proprties not set");
        }
    }

    private void checkSolrConfiguration(String solrHomePath, String solrConfigPath, String coreName) throws IOException {

        // check if solrHomePath exists
        File solrHomePathFile = new File(solrHomePath);
        if (!solrHomePathFile.exists()) {
            if (!solrHomePathFile.mkdirs()) {
                throw new IOException("could not create solrHomePath directory");
            } else {
                // copy all the needed files to the just created directory
                copy("/solr/solr.xml", solrHomePath);
                copy("/solr/zoo.cfg", solrHomePath);
                if (!new File(solrHomePath + "/oak/conf").mkdirs()) {
                    throw new IOException("could not create nested core directory in solrHomePath");
                }
                String coreDir = solrHomePath + "/oak/conf/";
                copy("/solr/oak/conf/currency.xml", coreDir);
                copy("/solr/oak/conf/schema.xml", coreDir);
                copy("/solr/oak/conf/solrconfig.xml", coreDir);
            }
        } else if (!solrHomePathFile.isDirectory()) {
            throw new IOException("a non directory file with the specified name already exists for the given solrHomePath");
        }

        File solrConfigPathFile = new File(solrConfigPath);
        // check if solrConfigPath exists
        if (!solrConfigPathFile.exists()) {
            if (solrConfigPathFile.createNewFile()) {
                copy("/solr/solr.xml", solrConfigPathFile.getAbsolutePath());
            }
        }

        // check if the a core with the given coreName exists
        // TODO : improve this check
        String[] files = new File(solrHomePath).list();
        Arrays.sort(files);
        if (Arrays.binarySearch(files, coreName) < 0) {
            throw new IOException("could not find a directory with the given coreName in the solrHomePath");
        }


    }

    private void copy(String resource, String dir) throws IOException {
        String fileName = dir + resource.substring(resource.lastIndexOf("/"));
        File outputFile = new File(fileName);
        if (outputFile.createNewFile()) {
            InputStream inputStream = null;
            FileOutputStream outputStream = null;
            try {
                inputStream = getClass().getResourceAsStream(resource);
                outputStream = new FileOutputStream(outputFile);
                IOUtils.copy(inputStream, outputStream);
            } finally {
                if (inputStream != null) {
                    try {
                        inputStream.close();
                    } catch (Exception e) {
                        // do nothing
                    }
                }
                if (outputStream != null) {
                    try {
                        outputStream.close();
                    } catch (Exception e) {
                        // do nothing
                    }
                }
            }
        }

    }

    @Override
    public SolrServer getSolrServer() throws Exception {
        if (solrServer == null) {
            solrServer = createSolrServer();
        }
        return solrServer;
    }

    @Override
    public OakSolrConfiguration getConfiguration() {
        return oakSolrConfiguration;
    }
}
