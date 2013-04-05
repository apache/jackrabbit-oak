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
package org.apache.jackrabbit.oak.plugins.index.solr.embedded;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.jackrabbit.mk.util.IOUtils;
import org.apache.jackrabbit.oak.plugins.index.solr.OakSolrUtils;
import org.apache.jackrabbit.oak.plugins.index.solr.SolrServerProvider;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.core.CoreContainer;

/**
 * Default implementation of {@link SolrServerProvider} which uses an
 * {@link EmbeddedSolrServer} configured as per passed {@link SolrServerConfiguration}.
 */
public class EmbeddedSolrServerProvider implements SolrServerProvider {

    private final SolrServerConfiguration solrServerConfiguration;

    public EmbeddedSolrServerProvider(SolrServerConfiguration solrServerConfiguration) {
        this.solrServerConfiguration = solrServerConfiguration;
    }

    private SolrServer solrServer;

    private SolrServer createSolrServer() throws Exception {

        String solrHomePath = solrServerConfiguration.getSolrHomePath();
        String coreName = solrServerConfiguration.getCoreName();
        String solrConfigPath = solrServerConfiguration.getSolrConfigPath();
        SolrServerConfiguration.HttpConfiguration httpConfiguration = solrServerConfiguration.getHttpConfiguration();


        if (solrConfigPath != null && solrHomePath != null && coreName != null) {
            checkSolrConfiguration(solrHomePath, solrConfigPath, coreName);
            if (httpConfiguration != null) {
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                Thread.currentThread().setContextClassLoader(JettySolrRunner.class.getClassLoader());

                Integer httpPort = httpConfiguration.getHttpPort();
                String context = httpConfiguration.getContext();
                try {
                    JettySolrRunner jettySolrRunner = new JettySolrRunner(solrHomePath, context, httpPort, "solrconfig.xml", "schema.xml", true);
                    jettySolrRunner.start(true);
                } finally {
                    Thread.currentThread().setContextClassLoader(classLoader);
                }
                HttpSolrServer httpSolrServer = new HttpSolrServer(new StringBuilder(SolrServerConfigurationDefaults.LOCAL_BASE_URL)
                        .append(':').append(httpPort).append(coreName).toString());
                if (OakSolrUtils.checkServerAlive(httpSolrServer)) {
                    return httpSolrServer;
                } else {
                    throw new IOException("the spawn HTTP Solr server is not alive");
                }


            } else {
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                Thread.currentThread().setContextClassLoader(CoreContainer.class.getClassLoader());

                CoreContainer coreContainer = new CoreContainer(solrHomePath);
                try {
                    coreContainer.load(solrHomePath, new File(solrConfigPath));
                } finally {
                    Thread.currentThread().setContextClassLoader(classLoader);
                }

                EmbeddedSolrServer server = new EmbeddedSolrServer(coreContainer, coreName);
                if (OakSolrUtils.checkServerAlive(server)) {
                    return server;
                } else {
                    throw new IOException("the embedded Solr server is not alive");
                }
            }
        } else {
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

}
