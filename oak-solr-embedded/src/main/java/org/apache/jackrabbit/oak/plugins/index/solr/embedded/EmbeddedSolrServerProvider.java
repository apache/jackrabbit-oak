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
import java.io.IOException;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.plugins.index.solr.OakSolrUtils;
import org.apache.jackrabbit.oak.plugins.index.solr.SolrServerProvider;
import org.apache.lucene.codecs.Codec;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.core.CoreContainer;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 * {@link SolrServerProvider} which (spawns if needed and) exposes an embedded Solr server
 */
@Component(metatype = true, immediate = true)
@Service(SolrServerProvider.class)
public class EmbeddedSolrServerProvider implements SolrServerProvider {

    private final Logger log = LoggerFactory.getLogger(EmbeddedSolrServerProvider.class);

    private static final String DEFAULT_PORT = "8983";
    private static final String DEFAULT_HOME_PATH = "/";
    private static final String DEFAULT_CORE_NAME = "oak";
    private static final String SOLR_HOME_PROPERTY_NAME = "solr.solr.home";
    private static final String LOCAL_BASE_URL = "http://127.0.0.1";
    private static final String CONTEXT = "/solr";

    @Property(value = DEFAULT_HOME_PATH)
    private static final String SOLR_HOME_PATH = "solr.home.path";

    @Property(value = DEFAULT_PORT)
    private static final String SOLR_HTTP_PORT = "solr.http.port";

    private static SolrServer solrServer;

    private String solrHome;
    private Integer solrHttpPort;

    public EmbeddedSolrServerProvider() {
        this.solrHome = DEFAULT_HOME_PATH;
        this.solrHttpPort = Integer.valueOf(DEFAULT_PORT);
    }

    public EmbeddedSolrServerProvider(String solrHome, Integer solrHttpPort) {
        this.solrHome = solrHome;
        this.solrHttpPort = solrHttpPort;
    }

    @Activate
    protected void activate(ComponentContext componentContext) throws Exception {
        solrHome = String.valueOf(componentContext.getProperties().get(SOLR_HOME_PATH));
        File file = new File(solrHome);
        if (!file.exists()) {
            assert file.createNewFile();
        }
        solrHttpPort = Integer.valueOf(String.valueOf(componentContext.getProperties().get(SOLR_HTTP_PORT)));
        // TODO : add the possibility to inject solrconfig and schema files
    }

    @Deactivate
    protected void deactivate() throws Exception {
        solrHome = null;
        solrHttpPort = null;
        if (solrServer != null) {
            solrServer.shutdown();
            solrServer = null;
        }
    }

    @Override
    public SolrServer getSolrServer() throws Exception {
        // hack needed to let lucene SPIs work in an OSGi deploy
        Codec.reloadCodecs(Thread.currentThread().getContextClassLoader());

        if (solrServer == null) {
            try {
                solrServer = initializeWithNewHttpServer();
            } catch (Exception e) {
                log.warn("unable to spawn a new Solr server and initialize the default Solr HTTP client");
                try {
                    solrServer = initializeWithEmbeddedSolrServer();
                } catch (Exception e2) {
                    log.warn("unable to initialize embedded Solr client");
                    throw new IOException("unable to initialize an embedded Solr server", e2);
                }
            }
            if (solrServer == null) {
                throw new IOException("could not connect to any embedded Solr server");
            }
        }
        return solrServer;
    }

    private SolrServer initializeWithNewHttpServer() throws Exception {
        // try spawning a new Solr server using Jetty and connect to it via HTTP

        enableSolrCloud(solrHome, DEFAULT_CORE_NAME);

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(JettySolrRunner.class.getClassLoader());

        try {
            JettySolrRunner jettySolrRunner = new JettySolrRunner(solrHome, CONTEXT, solrHttpPort, "solrconfig.xml", "schema.xml", true);
            jettySolrRunner.start(true);
        } finally {
            Thread.currentThread().setContextClassLoader(classLoader);
        }
        HttpSolrServer httpSolrServer = new HttpSolrServer(new StringBuilder(LOCAL_BASE_URL)
                .append(':').append(solrHttpPort).append(CONTEXT).toString());
        if (OakSolrUtils.checkServerAlive(httpSolrServer)) {
            return httpSolrServer;
        } else {
            throw new IOException("the spawn HTTP Solr server is not alive");
        }

    }

    private SolrServer initializeWithEmbeddedSolrServer() throws IOException, ParserConfigurationException, SAXException, SolrServerException {
        // fallback to creating an in memory bundled Solr instance
        System.setProperty(SOLR_HOME_PROPERTY_NAME, solrHome);
        enableSolrCloud(solrHome, DEFAULT_CORE_NAME);
        CoreContainer.Initializer initializer = new CoreContainer.Initializer();

        EmbeddedSolrServer embeddedSolrServer = new EmbeddedSolrServer(initializer.initialize(), DEFAULT_CORE_NAME);
        if (OakSolrUtils.checkServerAlive(embeddedSolrServer)) {
            return embeddedSolrServer;
        } else {
            throw new IOException("the found embedded Solr server is not alive");
        }

    }

    private void enableSolrCloud(String solrHome, String coreName) {
        // TODO : expose such properties via OSGi conf
        if (System.getProperty("solrcloud") != null && System.getProperty("solrcloud").equals("true")) {
            // enable embedded SolrCloud by setting needed params
            System.setProperty("zkRun", "true");
//        System.setProperty("bootstrap_conf", "true");
            System.setProperty("bootstrap_confdir", solrHome + "/" + coreName + "/conf");
            System.setProperty("collection.configName", "oakconf");
//        System.setProperty("numShards", "1");
        }
    }
}
