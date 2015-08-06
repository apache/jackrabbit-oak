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

import org.apache.jackrabbit.rmi.client.ClientRepositoryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Properties;

import javax.jcr.Repository;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

/**
 * This Class implements a servlet that is used as unified mechanism to retrieve
 * a jcr repository either through JNDI or RMI.
 */
public class RepositoryAccessServlet extends HttpServlet {

    /**
     * default logger
     */
    private static final Logger log = LoggerFactory.getLogger(RepositoryAccessServlet.class);

    /**
     * initial param name for the bootstrap config location
     */
    public final static String INIT_PARAM_BOOTSTRAP_CONFIG = "bootstrap-config";

    /**
     * Context parameter name for 'this' instance.
     */
    private final static String CTX_PARAM_THIS = "repository.access.servlet";

    /**
     * Ugly hack to override the bootstrap file location in the test cases
     */
    static String bootstrapOverride = null;

    /**
     * the bootstrap config
     */
    private BootstrapConfig config;

    /**
     * the initialized initial context
     */
    private InitialContext jndiContext;

    /**
     * if this is set we try to get a Repository from the ServletContext
     */
    private String repositoryContextAttributeName;

    /**
     * the repository
     */
    private Repository repository;

    /**
     * Initializes the servlet.<br>
     * Please note that only one repository startup servlet may exist per
     * webapp. it registers itself as context attribute and acts as singleton.
     *
     * @throws ServletException if a same servlet is already registered or of
     * another initialization error occurs.
     */
    public void init() throws ServletException {
        // check if servlet is defined twice
        if (getServletContext().getAttribute(CTX_PARAM_THIS) !=  null) {
            throw new ServletException("Only one repository access servlet allowed per web-app.");
        }
        getServletContext().setAttribute(CTX_PARAM_THIS, this);

        repositoryContextAttributeName = getServletConfig().getInitParameter("repository.context.attribute.name");

        log.info("RepositoryAccessServlet initialized.");
    }

    /**
     * Returns the instance of this servlet
     * @param ctx the servlet context
     * @return this servlet
     */
    public static RepositoryAccessServlet getInstance(ServletContext ctx) {
        final RepositoryAccessServlet instance = (RepositoryAccessServlet) ctx.getAttribute(CTX_PARAM_THIS);
        if(instance==null) {
            throw new IllegalStateException(
                "No RepositoryAccessServlet instance in ServletContext, RepositoryAccessServlet servlet not initialized?"
            );
        }
        return instance;
    }

    /**
     * Returns the bootstrap config
     * @return the bootstrap config
     * @throws ServletException if the config is not valid
     */
    private BootstrapConfig getConfig() throws ServletException {
        if (config == null) {
            // check if there is a loadable bootstrap config
            Properties bootstrapProps = new Properties();
            String bstrp = bootstrapOverride;
            if (bstrp == null) {
                bstrp = getServletConfig().getInitParameter(INIT_PARAM_BOOTSTRAP_CONFIG);
            }
            if (bstrp != null) {
                // check if it's a web-resource
                InputStream in = getServletContext().getResourceAsStream(bstrp);
                if (in == null) {
                    // check if it's a file
                    File file = new File(bstrp);
                    if (file.canRead()) {
                        try {
                            in = new FileInputStream(file);
                        } catch (FileNotFoundException e) {
                            throw new ServletExceptionWithCause(
                                    "Bootstrap configuration not found: " + bstrp, e);
                        }
                    }
                }
                if (in != null) {
                    try {
                        bootstrapProps.load(in);
                    } catch (IOException e) {
                        throw new ServletExceptionWithCause(
                                "Bootstrap configuration failure: " + bstrp, e);
                    } finally {
                        try {
                            in.close();
                        } catch (IOException e) {
                            // ignore
                        }
                    }
                }
            }

            // read bootstrap config
            BootstrapConfig tmpConfig = new BootstrapConfig();
            tmpConfig.init(getServletConfig());
            tmpConfig.init(bootstrapProps);
            tmpConfig.validate();
            if (!tmpConfig.isValid()) {
                throw new ServletException(
                        "Repository access configuration is not valid.");
            }
            tmpConfig.logInfos();
            config = tmpConfig;
        }
        return config;
    }

    /**
     * Returns the initial jndi context or <code>null</code> if the jndi access
     * is not configured or erroous.
     * @return the initial context or <code>null</code>
     */
    private InitialContext getInitialContext() {
        if (jndiContext == null && config.getJndiConfig().enabled()) {
            // retrieve JNDI Context environment
            try {
                jndiContext = new InitialContext(config.getJndiConfig().getJndiEnv());
            } catch (NamingException e) {
                log.error("Create initial context: " + e.toString());
            }
        }
        return jndiContext;
    }

    /**
     * Checks if the repository is available via JNDI and returns it.
     * @return the repository or <code>null</code>
     * @throws ServletException if this servlet is not properly configured.
     */
    private Repository getRepositoryByJNDI() throws ServletException {
        BootstrapConfig config = getConfig();
        if (!config.getJndiConfig().isValid() || !config.getJndiConfig().enabled()) {
            return null;
        }
        // acquire via JNDI
        String repositoryName = config.getRepositoryName();
        InitialContext ctx = getInitialContext();
        if (ctx == null) {
            return null;
        }
        try {
            Repository r = (Repository) ctx.lookup(repositoryName);
            log.info("Acquired repository via JNDI.");
            return r;
        } catch (NamingException e) {
            log.error("Error while retrieving repository using JNDI (name={})", repositoryName, e);
            return null;
        }
    }

    /**
     * Checks if the repository is available via RMI and returns it.
     * @return the repository or <code>null</code>
     * @throws ServletException if this servlet is not properly configured.
     */
    private Repository getRepositoryByRMI() throws ServletException {
        BootstrapConfig config = getConfig();
        if (!config.getRmiConfig().isValid() || !config.getRmiConfig().enabled()) {
            return null;
        }

        // acquire via RMI
        String rmiURI = config.getRmiConfig().getRmiUri();
        if (rmiURI == null) {
            return null;
        }
        log.info("  trying to retrieve repository using rmi. uri={}", rmiURI);
        ClientFactoryDelegater cfd;
        try {
            Class clazz = Class.forName(getServerFactoryDelegaterClass());
            cfd = (ClientFactoryDelegater) clazz.newInstance();
        } catch (Throwable e) {
            log.error("Unable to locate RMI ClientRepositoryFactory. Is jcr-rmi.jar missing?", e);
            return null;
        }

        try {
            Repository r = cfd.getRepository(rmiURI);
            log.info("Acquired repository via RMI.");
            return r;
        } catch (Exception e) {
            log.error("Error while retrieving repository using RMI: {}", rmiURI, e);
            return null;
        }
    }

    /**
     *  If our config said so, try to retrieve a Repository from the ServletContext
     */
    protected Repository getRepositoryByContextAttribute() {
        Repository result = null;
        if(repositoryContextAttributeName!=null) {
            result = (Repository)getServletContext().getAttribute(repositoryContextAttributeName);

            if(log.isDebugEnabled()) {
                if(result!=null) {
                    log.debug("Got Repository from ServletContext attribute '{}'", repositoryContextAttributeName);
                } else {
                    log.debug("ServletContext attribute '{}' does not provide a Repository", repositoryContextAttributeName);
                }
            }
        }
        return result;
    }

    /**
     * Return the fully qualified name of the class providing the client
     * repository. The class whose name is returned must implement the
     * {@link ClientFactoryDelegater} interface.
     *
     * @return the qfn of the factory class.
     */
    protected String getServerFactoryDelegaterClass() {
        return getClass().getName() + "$RMIClientFactoryDelegater";
    }

    /**
     * Returns the JCR repository
     *
     * @return a JCR repository
     * @throws IllegalStateException if the repository is not available in the context.
     */
    public Repository getRepository() {
        try {
            if (repository == null) {
                // try to get via context attribute
                repository = getRepositoryByContextAttribute();
            }
            if (repository == null) {
                // try to retrieve via jndi
                repository = getRepositoryByJNDI();
            }
            if (repository == null) {
                // try to get via rmi
                repository = getRepositoryByRMI();
            }
            if (repository == null) {
                throw new ServletException("N/A");
            }
            return repository;
        } catch (ServletException e) {
            throw new IllegalStateException(
                    "The repository is not available. Please check"
                    + " RepositoryAccessServlet configuration in web.xml.", e);
        }
    }

    /**
     * Returns the JCR repository
     *
     * @param ctx the servlet context
     * @return a JCR repository
     * @throws IllegalStateException if the repository is not available in the context.
     */
    public static Repository getRepository(ServletContext ctx) {
        return getInstance(ctx).getRepository();
    }

    /**
     * Returns the config that was used to bootstrap this servlet.
     * @return the bootstrap config or <code>null</code>.
     */
    public BootstrapConfig getBootstrapConfig() {
        return config;
    }

    /**
     * optional class for RMI, will only be used, if RMI client is present
     */
    protected static abstract class ClientFactoryDelegater {

        public abstract Repository getRepository(String uri)
                throws RemoteException, MalformedURLException, NotBoundException;
    }

    /**
     * optional class for RMI, will only be used, if RMI server is present
     */
    protected static class RMIClientFactoryDelegater extends ClientFactoryDelegater {

        // only used to enforce linking upon Class.forName()
        static String FactoryClassName = ClientRepositoryFactory.class.getName();

        public Repository getRepository(String uri)
                throws MalformedURLException, NotBoundException, RemoteException {
            System.setProperty("java.rmi.server.useCodebaseOnly", "true");
            return new ClientRepositoryFactory().getRepository(uri);
        }
    }
}

