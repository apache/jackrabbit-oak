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
package org.apache.jackrabbit.j2ee;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import junit.framework.TestCase;

import org.apache.catalina.startup.Tomcat;
import org.apache.commons.io.FileUtils;
import org.apache.http.conn.HttpHostConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlElement;
import com.gargoylesoftware.htmlunit.html.HtmlForm;
import com.gargoylesoftware.htmlunit.html.HtmlInput;
import com.gargoylesoftware.htmlunit.html.HtmlPage;

public class TomcatIT extends TestCase {

    static {
        SLF4JBridgeHandler.install();
    }

    private static Logger LOG = LoggerFactory.getLogger(TomcatIT.class);
    
    private URL url;

    private Tomcat tomcat;

    private WebClient client;

    protected void setUp() throws Exception {
        File war = null;
        for (File f : new File("target").listFiles()) {
            if (f.isDirectory() && new File(f, "WEB-INF/web.xml").isFile()) {
                war = f;
                break;
            }
        }
        assertNotNull(war);

        File bootstrap = new File("target", "bootstrap.properties");
        bootstrap.delete();
        RepositoryAccessServlet.bootstrapOverride = bootstrap.getPath();
        RepositoryStartupServlet.bootstrapOverride = bootstrap.getPath();

        File baseDir = new File("target", "tomcat");
        FileUtils.deleteQuietly(baseDir);

        File repoDir = new File("target", "repository");
        FileUtils.deleteQuietly(repoDir);

        url = new URL("http://localhost:"+getPort()+"/");

        tomcat = new Tomcat();
        tomcat.setSilent(true);
        tomcat.setBaseDir(baseDir.getPath());
        tomcat.setHostname(url.getHost());
        tomcat.setPort(url.getPort());

        tomcat.addWebapp("", war.getAbsolutePath());

        tomcat.start();

        client = new WebClient();
    }

    public void testTomcat() throws Exception {
        HtmlPage page = null;
        
        try {
            page = client.getPage(url);
        } catch (HttpHostConnectException e) {
            // sometimes on jenkins there are connections exceptions.
            // ignoring the rest of the test in this case
            LOG.error("Failed connecting to tomcat", e);
            return;
        }
        
        assertEquals("Content Repository Setup", page.getTitleText());

        page = submitNewRepositoryForm(page);
        assertEquals("Content Repository Ready", page.getTitleText());

        page = page.getAnchorByText("home").click();
        assertEquals("Apache Jackrabbit JCR Server", page.getTitleText());
    }

    private HtmlPage submitNewRepositoryForm(HtmlPage page) throws IOException {
        for (HtmlForm form : page.getForms()) {
            for (HtmlInput mode : form.getInputsByName("mode")) {
                if ("new".equals(mode.getValueAttribute())) {
                    for (HtmlInput home : form.getInputsByName("repository_home")) {
                        home.setValueAttribute("target/repository");
                        for (HtmlElement submit : form.getElementsByAttribute("input", "type", "submit")) {
                            return submit.click();
                        }
                    }
                }
            }
        }
        fail();
        return null;
    }

    protected void tearDown() throws Exception {
        client.close();

        tomcat.stop();
    }

    private static int getPort() {
        return Integer.getInteger("tomcat.http.port", 12856);
    }

}
