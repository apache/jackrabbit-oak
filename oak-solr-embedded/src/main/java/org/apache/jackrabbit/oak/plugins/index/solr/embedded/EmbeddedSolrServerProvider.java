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

import org.apache.felix.scr.annotations.Deactivate;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationDefaults;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link SolrServerProvider} which uses an
 * {@link EmbeddedSolrServer} configured as per passed {@link SolrServerConfiguration}.
 */
public class EmbeddedSolrServerProvider implements SolrServerProvider {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final SolrServerConfiguration solrServerConfiguration;

    public EmbeddedSolrServerProvider(SolrServerConfiguration solrServerConfiguration) {
        this.solrServerConfiguration = solrServerConfiguration;
    }

    private SolrServer solrServer;

    @Deactivate
    protected void deactivate() throws Exception {
        if (solrServer != null) {
            solrServer.shutdown();
        }
    }

    private SolrServer createSolrServer() throws Exception {

        String solrHomePath = solrServerConfiguration.getSolrHomePath();
        String coreName = solrServerConfiguration.getCoreName();
        String solrConfigPath = solrServerConfiguration.getSolrConfigPath();
        SolrServerConfiguration.HttpConfiguration httpConfiguration = solrServerConfiguration.getHttpConfiguration();


        if (solrConfigPath != null && solrHomePath != null && coreName != null) {
            checkSolrConfiguration(solrHomePath, solrConfigPath, coreName);
            if (httpConfiguration != null) {
                if (log.isInfoEnabled()) {
                    log.info("starting embedded Solr server with http bindings");
                }
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                Thread.currentThread().setContextClassLoader(JettySolrRunner.class.getClassLoader());

                Integer httpPort = httpConfiguration.getHttpPort();
                String context = httpConfiguration.getContext();
                JettySolrRunner jettySolrRunner = null;
                try {
                    jettySolrRunner = new JettySolrRunner(solrHomePath, context, httpPort, "solrconfig.xml", "schema.xml", true);
                    if (log.isInfoEnabled()) {
                        log.info("Jetty runner instantiated");
                    }
                    jettySolrRunner.start(true);
                    if (log.isInfoEnabled()) {
                        log.info("Jetty runner started");
                    }
                } catch (Exception t) {
                    if (log.isErrorEnabled()) {
                        log.error("an error has occurred while starting Solr Jetty", t);
                    }
                } finally {
                    if (jettySolrRunner != null && !jettySolrRunner.isRunning()) {
                        try {
                            jettySolrRunner.stop();
                            if (log.isInfoEnabled()) {
                                log.info("Jetty runner stopped");
                            }
                        } catch (Exception e) {
                            if (log.isErrorEnabled()) {
                                log.error("error while stopping the Jetty runner", e);
                            }
                        }
                    }
                    Thread.currentThread().setContextClassLoader(classLoader);
                }
                if (log.isInfoEnabled()) {
                    log.info("starting HTTP Solr server");
                }
                HttpSolrServer httpSolrServer = new HttpSolrServer(new StringBuilder(
                        SolrServerConfigurationDefaults.LOCAL_BASE_URL).append(':')
                        .append(httpPort).append(context).append('/').append(coreName)
                        .toString());
                return httpSolrServer;
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
                if (server.ping().getStatus() == 0) {
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
                if (!new File(solrHomePath + "/" + coreName + "/conf").mkdirs()) {
                    throw new IOException("could not create nested core directory in solrHomePath");
                }
                String coreDir = solrHomePath + "/" + coreName + "/conf/";
                copy("/solr/oak/conf/currency.xml", coreDir);
                copy("/solr/oak/conf/schema.xml", coreDir);
                copy("/solr/oak/conf/solrconfig.xml", coreDir);
            }
        } else if (!solrHomePathFile.isDirectory()) {
            throw new IOException("a non directory file with the specified name already exists for the given solrHomePath '"+solrHomePath);
        }

        // TODO : improve this check
        // check if solrConfigPath exists
//        File solrConfigPathFile = new File(solrConfigPath);
//        if (!solrConfigPathFile.exists()) {
//            if (solrConfigPathFile.createNewFile()) {
//                copy("/solr/solr.xml", solrConfigPathFile.getAbsolutePath());
//            }
//        }

        // check if the a core with the given coreName exists
        // TODO : improve this check
        String[] files = new File(solrHomePath).list();
        Arrays.sort(files);
        if (Arrays.binarySearch(files, coreName) < 0) {
            throw new IOException("could not find a directory with the coreName '" + coreName
                    + "' in the solrHomePath '" + solrHomePath + "'");
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
        synchronized (this) {
            if (solrServer == null) {
                solrServer = createSolrServer();
            }
        }
        return solrServer;
    }

}
