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

import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.jcr.Repository;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.benchmark.BenchmarkRunner;
import org.apache.jackrabbit.oak.http.OakServlet;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.webdav.jcr.JCRWebdavServerServlet;
import org.apache.jackrabbit.webdav.server.AbstractWebdavServlet;
import org.apache.jackrabbit.webdav.simple.SimpleWebdavServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class Main {

    public static final int PORT = 8080;
    public static final String URI = "http://localhost:" + PORT + "/";

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        printProductInfo();

        String command = "server";
        if (args.length > 0) {
            command = args[0];
            String[] tail = new String[args.length - 1];
            System.arraycopy(args, 1, tail, 0, tail.length);
            args = tail;
        }
        if ("mk".equals(command)) {
            MicroKernelServer.main(args);
        } else if ("benchmark".equals(command)){
            BenchmarkRunner.main(args);
        } else if ("server".equals(command)){
            new HttpServer(URI, args);
        } else {
            System.err.println("Unknown command: " + command);
            System.exit(1);
        }
    }

    private static void printProductInfo() {
        String version = null;

        try {
            InputStream stream = Main.class
                    .getResourceAsStream("/META-INF/maven/org.apache.jackrabbit/oak-run/pom.properties");
            if (stream != null) {
                try {
                    Properties properties = new Properties();
                    properties.load(stream);
                    version = properties.getProperty("version");
                } finally {
                    stream.close();
                }
            }
        } catch (Exception ignore) {
        }

        String product;
        if (version != null) {
            product = "Apache Jackrabbit Oak " + version;
        } else {
            product = "Apache Jackrabbit Oak";
        }

        System.out.println(product);
    }

    public static class HttpServer {

        private final ServletContextHandler context;

        private final Server server;

        private final MicroKernel[] kernels;

        private final ScheduledExecutorService executor;

        public HttpServer(String uri, String[] args) throws Exception {
            int port = java.net.URI.create(uri).getPort();
            if (port == -1) {
                // use default
                port = PORT;
            }

            context = new ServletContextHandler();
            context.setContextPath("/");

            executor = Executors.newScheduledThreadPool(3);

            if (args.length == 0) {
                System.out.println("Starting an in-memory repository");
                System.out.println(uri + " -> [memory]");
                kernels = new MicroKernel[] { new MicroKernelImpl() };
                addServlets(new KernelNodeStore(kernels[0]), "");
            } else if (args.length == 1) {
                System.out.println("Starting a standalone repository");
                System.out.println(uri + " -> " + args[0]);
                kernels = new MicroKernel[] { new MicroKernelImpl(args[0]) };
                addServlets(new KernelNodeStore(kernels[0]), "");
            } else {
                System.out.println("Starting a clustered repository");
                kernels = new MicroKernel[args.length];
                for (int i = 0; i < args.length; i++) {
                    // FIXME: Use a clustered MicroKernel implementation
                    System.out.println(uri + "/node" + i + "/ -> " + args[i]);
                    kernels[i] = new MicroKernelImpl(args[i]);
                    addServlets(new KernelNodeStore(kernels[i]), "/node" + i);
                }
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
            executor.shutdown();
        }

        private void addServlets(NodeStore store, String path) {
            Oak oak = new Oak(store);
            Jcr jcr = new Jcr(oak).with(executor);

            ContentRepository repository = oak.createContentRepository();

            ServletHolder holder =
                    new ServletHolder(new OakServlet(repository));
            context.addServlet(holder, path + "/*");

            final Repository jcrRepository = jcr.createRepository();

            ServletHolder webdav =
                    new ServletHolder(new SimpleWebdavServlet() {
                        @Override
                        public Repository getRepository() {
                            return jcrRepository;
                        }
                    });
            webdav.setInitParameter(
                    SimpleWebdavServlet.INIT_PARAM_RESOURCE_PATH_PREFIX,
                    path + "/webdav");
            webdav.setInitParameter(
                    AbstractWebdavServlet.INIT_PARAM_AUTHENTICATE_HEADER,
                    "Basic realm=\"Oak\"");
            context.addServlet(webdav, path + "/webdav/*");

            ServletHolder davex =
                    new ServletHolder(new JCRWebdavServerServlet() {
                        @Override
                        protected Repository getRepository() {
                            return jcrRepository;
                        }
                    });
            davex.setInitParameter(
                    JCRWebdavServerServlet.INIT_PARAM_RESOURCE_PATH_PREFIX,
                    path + "/davex");
            webdav.setInitParameter(
                    AbstractWebdavServlet.INIT_PARAM_AUTHENTICATE_HEADER,
                    "Basic realm=\"Oak\"");
            context.addServlet(davex, path + "/davex/*");
        }

    }

}
