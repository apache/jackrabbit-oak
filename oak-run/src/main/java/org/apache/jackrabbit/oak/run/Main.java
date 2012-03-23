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

import javax.jcr.Repository;
import javax.servlet.Servlet;

import org.apache.jackrabbit.oak.jcr.GlobalContext;
import org.apache.jackrabbit.oak.jcr.configuration.OakRepositoryConfiguration;
import org.apache.jackrabbit.oak.jcr.configuration.RepositoryConfiguration;
import org.apache.jackrabbit.webdav.jcr.JCRWebdavServerServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class Main {

    public static void main(String[] args) throws Exception {
        String url = "mem:oak";
        if (args.length > 0) {
            url = args[0];
        }

        RepositoryConfiguration configuration =
                OakRepositoryConfiguration.create(Collections.singletonMap(
                        RepositoryConfiguration.MICROKERNEL_URL, url));
        final Repository repository =
                new GlobalContext(configuration).getInstance(Repository.class);
        Servlet servlet = new JCRWebdavServerServlet() {
            @Override
            protected Repository getRepository() {
                return repository;
            }
        };

        ServletContextHandler context =
                new ServletContextHandler(ServletContextHandler.SECURITY);
        context.setContextPath("/");
        context.addServlet(new ServletHolder(servlet),"/*");
 
        Server server = new Server(8080);
        server.setHandler(context);
        server.start();
        server.join();
    }

}
