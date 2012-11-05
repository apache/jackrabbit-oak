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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Executors;

import javax.jcr.Repository;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.blobs.BlobStore;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.mongomk.api.NodeStore;
import org.apache.jackrabbit.mongomk.impl.BlobStoreMongo;
import org.apache.jackrabbit.mongomk.impl.MongoConnection;
import org.apache.jackrabbit.mongomk.impl.MongoMicroKernel;
import org.apache.jackrabbit.mongomk.impl.NodeStoreMongo;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.http.OakServlet;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.RepositoryImpl;
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexHookProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexHookManager;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexHookProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexHookProvider;
import org.apache.jackrabbit.oak.plugins.name.NameValidatorProvider;
import org.apache.jackrabbit.oak.plugins.name.NamespaceValidatorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.DefaultTypeEditor;
import org.apache.jackrabbit.oak.plugins.nodetype.RegistrationValidatorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeValidatorProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.CompositeValidatorProvider;
import org.apache.jackrabbit.oak.spi.commit.ValidatingHook;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.webdav.jcr.JCRWebdavServerServlet;
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

        if (args.length > 0 && "mk".equals(args[0])) {
            String[] newArgs = new String[args.length - 1];
            System.arraycopy(args, 1, newArgs, 0, newArgs.length);
            MicroKernelServer.main(newArgs);
        } else {
            HttpServer httpServer = new HttpServer(URI, args);
            httpServer.start();
        }
    }

    private static MongoConnection createDefaultMongoConnection() throws Exception {
        // defaults
    	String host = "localhost";
    	int port = 27017;
    	String database = "MongoMicroKernel";
    	String configFile = "config.cfg";
    	
    	// try config file
    	InputStream is = null;
    	try {
    		is = new FileInputStream(configFile);
    		Properties properties = new Properties();
    		properties.load(is);

    		host = properties.getProperty("host");
    		port = Integer.parseInt(properties.getProperty("port"));
    		database = properties.getProperty("db");
    	} catch (FileNotFoundException e) {
    		System.out.println("Config file '"+configFile+"' not found, using defaults.");
    	} catch (IOException e) {
    		System.out.println("Error while reading '"+configFile+"', using defaults: " + e.getMessage());
    		
      	} finally {
      		IOUtils.closeQuietly(is);
      	}
        return new MongoConnection(host, port, database);
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

        public HttpServer(String uri, String[] args) throws Exception {
            int port = java.net.URI.create(uri).getPort();
            if (port == -1) {
                // use default
                port = PORT;
            }

            context = new ServletContextHandler(ServletContextHandler.SECURITY);
            context.setContextPath("/");

            if (args.length == 0) {
                System.out.println("Starting an in-memory repository");
                System.out.println(uri + " -> [memory]");
                kernels = new MicroKernel[] { new MicroKernelImpl() };
                addServlets(kernels[0], "");
            } else if (args.length == 1) {
            	System.out.println("Starting a standalone repository");
            	if (args[0].startsWith("mongodb")) {
            		MongoConnection mongoConnection = createDefaultMongoConnection();

            		mongoConnection.initializeDB(true);
	                System.out.println(uri + " -> mongodb microkernel " + args[0]);

	                NodeStore nodeStore = new NodeStoreMongo(mongoConnection);
	                BlobStore blobStore = new BlobStoreMongo(mongoConnection);
	                
	                kernels = new MicroKernel[] { new MongoMicroKernel(nodeStore, blobStore) };
            		
            	} else {
	                System.out.println(uri + " -> h2 database " + args[0]);
	                kernels = new MicroKernel[] { new MicroKernelImpl(args[0]) };
            	}
            	addServlets(kernels[0], "");
            } else {
                System.out.println("Starting a clustered repository");
                kernels = new MicroKernel[args.length];
                for (int i = 0; i < args.length; i++) {
                    // FIXME: Use a clustered MicroKernel implementation
                    System.out.println(uri + "/node" + i + "/ -> " + args[i]);
                    kernels[i] = new MicroKernelImpl(args[i]);
                    addServlets(kernels[i], "/node" + i);
                }
            }

            server = new Server(port);
            server.setHandler(context);
        }

        public void start() throws Exception {
            server.start();
        }

        public void join() throws Exception {
            server.join();
        }

        public void stop() throws Exception {
            server.stop();
        }

        private void addServlets(MicroKernel kernel, String path) {
            // TODO: review usage of opensecurity provider (using default will cause BasicServerTest to fail. usage of a:a credentials)
            SecurityProvider securityProvider = new OpenSecurityProvider();
            ContentRepository repository = new Oak(kernel)
                .with(buildDefaultCommitHook())
                .with(securityProvider)
                .createContentRepository();

            ServletHolder oak =
                    new ServletHolder(new OakServlet(repository));
            context.addServlet(oak, path + "/*");

            final Repository jcrRepository = new Jcr(kernel).createRepository(); 
            		//new RepositoryImpl(
                    //repository, Executors.newScheduledThreadPool(1), securityProvider);

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
                    SimpleWebdavServlet.INIT_PARAM_MISSING_AUTH_MAPPING,
                    "admin:admin");
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
            davex.setInitParameter(
                    JCRWebdavServerServlet.INIT_PARAM_MISSING_AUTH_MAPPING,
                    "admin:admin");
            context.addServlet(davex, path + "/davex/*");
        }

        private static CommitHook buildDefaultCommitHook() {
            return new CompositeHook(
                    new DefaultTypeEditor(),
                    new ValidatingHook(createDefaultValidatorProvider()),
                    new IndexHookManager(
                            new CompositeIndexHookProvider(
                            new PropertyIndexHookProvider(), 
                            new LuceneIndexHookProvider())));
        }

        private static ValidatorProvider createDefaultValidatorProvider() {
            return new CompositeValidatorProvider(
                    new NameValidatorProvider(),
                    new NamespaceValidatorProvider(),
                    new TypeValidatorProvider(),
                    new RegistrationValidatorProvider(),
                    new ConflictValidatorProvider());
        }

    }

}
