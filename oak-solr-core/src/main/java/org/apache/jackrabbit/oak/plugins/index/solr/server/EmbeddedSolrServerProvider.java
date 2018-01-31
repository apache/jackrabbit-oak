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

import javax.annotation.CheckForNull;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.EmbeddedSolrServerConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationDefaults;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider} which uses an
 * {@link EmbeddedSolrServer} configured as per passed {@link org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfiguration}.
 */
public class EmbeddedSolrServerProvider implements SolrServerProvider {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final EmbeddedSolrServerConfiguration solrServerConfiguration;

    public EmbeddedSolrServerProvider(EmbeddedSolrServerConfiguration solrServerConfiguration) {
        this.solrServerConfiguration = solrServerConfiguration;
    }

    private SolrClient createSolrServer() throws Exception {

        log.info("creating new embedded solr server with config: {}", solrServerConfiguration);

        String solrHomePath = solrServerConfiguration.getSolrHomePath();
        String coreName = solrServerConfiguration.getCoreName();
        EmbeddedSolrServerConfiguration.HttpConfiguration httpConfiguration = solrServerConfiguration.getHttpConfiguration();

        if (solrHomePath != null && coreName != null) {
            checkSolrConfiguration(solrHomePath, coreName);
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
                    jettySolrRunner.start();
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
                return new HttpWithJettySolrServer(SolrServerConfigurationDefaults.LOCAL_BASE_URL + ':' + httpPort + context + '/' + coreName, jettySolrRunner);
            } else {
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                Thread.currentThread().setContextClassLoader(CoreContainer.class.getClassLoader());

                CoreContainer coreContainer = new CoreContainer(solrHomePath);
                try {
                    if (!coreContainer.isLoaded(coreName)) {
                        coreContainer.load();
                    }
                } catch (Exception e) {
                    log.error("cannot load core {}, shutting down embedded Solr..", coreName, e);
                    try {
                        coreContainer.shutdown();
                    } catch (Exception se) {
                        log.error("could not shutdown embedded Solr", se);
                    }
                    return null;
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

    private void checkSolrConfiguration(String solrHomePath, String coreName) throws IOException {
        File solrHomePathFile = new File(solrHomePath);

        log.info("checking configuration at {}", solrHomePathFile.getAbsolutePath());

        // check if solrHomePath exists
        if (!solrHomePathFile.exists()) {
            if (!solrHomePathFile.mkdirs()) {
                throw new IOException("could not create solrHomePath directory");
            } else {
                // copy all the needed files to the just created directory
                copy("/solr/solr.xml", solrHomePath);
                copy("/solr/zoo.cfg", solrHomePath);

            }
        } else if (!solrHomePathFile.isDirectory()) {
            throw new IOException("a non directory file with the specified name already exists for the given solrHomePath '" + solrHomePath);
        }

        File solrCorePathFile = new File(solrHomePathFile, coreName);
        if (!solrCorePathFile.exists()) {
            if (!new File(solrCorePathFile, "conf").mkdirs()) {
                throw new IOException("could not create nested core directory in solrHomePath/solrCoreName/conf");
            }
            String solrCoreDir = solrCorePathFile.getAbsolutePath();
            File coreProperties = new File(new File(solrCoreDir), "core.properties");
            assert coreProperties.createNewFile();
            FileOutputStream out = new FileOutputStream(coreProperties);
            IOUtils.writeBytes(out, ("name=" + coreName).getBytes("UTF-8"));
            out.flush();
            out.close();

            String coreConfDir = solrCoreDir + "/conf/";
            copy("/solr/oak/conf/schema.xml", coreConfDir);
            copy("/solr/oak/conf/solrconfig.xml", coreConfDir);
        } else if (!solrCorePathFile.isDirectory()) {
            throw new IOException("a non directory file with the specified name already exists for the given Solr core path'" + solrCorePathFile.getAbsolutePath());
        }
        // clean data dir
        File solrDataPathFile = new File(solrHomePathFile + "/" + coreName + "/data/index");
        if (solrDataPathFile.exists()) {
            log.debug("deleting stale lock files");
            File[] locks = solrDataPathFile.listFiles((dir, name) -> "write.lock".equals(name));
            log.debug("found {} lock files", locks.length);
            // remove eventaul previous lock files (e.g. due to ungraceful shutdown)
            if (locks.length > 0) {
                for (File f : locks) {
                    FileUtils.forceDelete(f);
                    log.debug("deleted {}", f.getAbsolutePath());
                }
            }
        }

        // check if the a core with the given coreName exists
        String[] files = solrHomePathFile.list();
        if (files != null) {
            Arrays.sort(files);
            if (Arrays.binarySearch(files, coreName) < 0) {
                throw new IOException("could not find a directory with the coreName '" + coreName
                        + "' in the solrHomePath '" + solrHomePath + "'");
            }
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

    @CheckForNull
    @Override
    public SolrClient getSolrServer() throws Exception {
        return createSolrServer();
    }

    @CheckForNull
    @Override
    public SolrClient getIndexingSolrServer() throws Exception {
        return getSolrServer();
    }

    @CheckForNull
    @Override
    public SolrClient getSearchingSolrServer() throws Exception {
        return getSolrServer();
    }

    private class HttpWithJettySolrServer extends HttpSolrServer {
        private final JettySolrRunner jettySolrRunner;

        public HttpWithJettySolrServer(String s, JettySolrRunner jettySolrRunner) {
            super(s);
            this.jettySolrRunner = jettySolrRunner;
        }

        @Override
        public void shutdown() {
            super.shutdown();
            try {
                if (jettySolrRunner != null) {
                    if (jettySolrRunner.isRunning()) {
                        jettySolrRunner.stop();
                    }
                }
            } catch (Exception e) {
                log.warn("could not stop JettySolrRunner {}", jettySolrRunner);
            }
        }
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
