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

package org.apache.jackrabbit.oak.run;

import java.util.Collections;
import java.util.Map;

import javax.jcr.Repository;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.http.OakServlet;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.server.remoting.davex.JcrRemotingServlet;
import org.apache.jackrabbit.webdav.jcr.JCRWebdavServerServlet;
import org.apache.jackrabbit.webdav.server.AbstractWebdavServlet;
import org.apache.jackrabbit.webdav.simple.SimpleWebdavServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

class HttpServer {

    private final ServletContextHandler context;

    private final Server server;

    public HttpServer(int port) throws Exception {
        this(port, Collections.singletonMap(new Oak(), ""));
    }

    public HttpServer(int port, Map<Oak, String> oakMap) throws Exception {
        context = new ServletContextHandler();
        context.setContextPath("/");

        for (Map.Entry<Oak, String> entry : oakMap.entrySet()) {
            addServlets(entry.getKey(), entry.getValue());
        }

        server = new Server(port);
        server.setHandler(context);
        server.start();
    }

    public void join() throws Exception {
        server.join();
    }

    public void stop() throws Exception {
        server.stop();
    }

    private void addServlets(Oak oak, String path) {
        Jcr jcr = new Jcr(oak);

        // 1 - OakServer
        ContentRepository repository = jcr.createContentRepository();
        ServletHolder holder = new ServletHolder(new OakServlet(repository));
        context.addServlet(holder, path + "/*");

        // 2 - Webdav Server on JCR repository
        final Repository jcrRepository = jcr.createRepository();
        @SuppressWarnings("serial")
        ServletHolder webdav = new ServletHolder(new SimpleWebdavServlet() {
            @Override
            public Repository getRepository() {
                return jcrRepository;
            }
        });
        webdav.setInitParameter(SimpleWebdavServlet.INIT_PARAM_RESOURCE_PATH_PREFIX, path + "/webdav");
        webdav.setInitParameter(AbstractWebdavServlet.INIT_PARAM_AUTHENTICATE_HEADER, "Basic realm=\"Oak\"");
        context.addServlet(webdav, path + "/webdav/*");

        // 3 - JCR Remoting Server
        @SuppressWarnings("serial")
        ServletHolder jcrremote = new ServletHolder(new JcrRemotingServlet() {
            @Override
            protected Repository getRepository() {
                return jcrRepository;
            }
        });
        jcrremote.setInitParameter(JCRWebdavServerServlet.INIT_PARAM_RESOURCE_PATH_PREFIX, path + "/jcrremote");
        jcrremote.setInitParameter(AbstractWebdavServlet.INIT_PARAM_AUTHENTICATE_HEADER, "Basic realm=\"Oak\"");
        context.addServlet(jcrremote, path + "/jcrremote/*");
    }

}
