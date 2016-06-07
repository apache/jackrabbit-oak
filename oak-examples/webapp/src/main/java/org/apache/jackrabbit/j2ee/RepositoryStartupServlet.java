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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.felix.connect.launch.PojoServiceRegistry;
import org.apache.felix.webconsole.WebConsoleSecurityProvider;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.commons.repository.RepositoryFactory;
import org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory;
import org.apache.jackrabbit.oak.run.osgi.ServiceRegistryProvider;
import org.apache.jackrabbit.rmi.server.RemoteAdapterFactory;
import org.apache.jackrabbit.rmi.server.ServerAdapterFactory;
import org.apache.jackrabbit.servlet.AbstractRepositoryServlet;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * The RepositoryStartupServlet starts a jackrabbit repository and registers it
 * to the JNDI environment and optional to the RMI registry.
 * <p id="registerAlgo">
 * <b>Registration with RMI</b>
 * <p>
 * Upon successfull creation of the repository in the {@link #init()} method,
 * the repository is registered with an RMI registry if the web application is
 * so configured. To register with RMI, the following web application
 * <code>init-params</code> are considered: <code>rmi-port</code> designating
 * the port on which the RMI registry is listening, <code>rmi-host</code>
 * designating the interface on the local host on which the RMI registry is
 * active, <code>repository-name</code> designating the name to which the
 * repository is to be bound in the registry, and <code>rmi-uri</code>
 * designating an RMI URI complete with host, optional port and name to which
 * the object is bound.
 * <p>
 * If the <code>rmi-uri</code> parameter is configured with a non-empty value,
 * the <code>rmi-port</code> and <code>rmi-host</code> parameters are ignored.
 * The <code>repository-name</code> parameter is only considered if a non-empty
 * <code>rmi-uri</code> parameter is configured if the latter does not contain
 * a name to which to bind the repository.
 * <p>
 * This is the algorithm used to find out the host, port and name for RMI
 * registration:
 * <ol>
 * <li>If neither a <code>rmi-uri</code> nor a <code>rmi-host</code> nor a
 * <code>rmi-port</code> parameter is configured, the repository is not
 * registered with any RMI registry.
 * <li>If a non-empty <code>rmi-uri</code> parameter is configured extract the
 * host name (or IP address), port number and name to bind to from the
 * URI. If the URI is not valid, host defaults to <code>0.0.0.0</code>
 * meaning all interfaces on the local host, port defaults to the RMI
 * default port (<code>1099</code>) and the name defaults to the value
 * of the <code>repository-name</code> parameter.
 * <li>If a non-empty <code>rmi-uri</code> is not configured, the host is taken
 * from the <code>rmi-host</code> parameter, the port from the
 * <code>rmi-port</code> parameter and the name to bind the repository to
 * from the <code>repository-name</code> parameter. If the
 * <code>rmi-host</code> parameter is empty or not configured, the host
 * defaults to <code>0.0.0.0</code> meaning all interfaces on the local
 * host. If the <code>rmi-port</code> parameter is empty, not configured,
 * zero or a negative value, the default port for the RMI registry
 * (<code>1099</code>) is used.
 * </ol>
 * <p>
 * After finding the host and port of the registry, the RMI registry itself
 * is acquired. It is assumed, that host and port primarily designate an RMI
 * registry, which should be active on the local host but has not been started
 * yet. In this case, the <code>LocateRegistry.createRegistry</code> method is
 * called to create a registry on the local host listening on the host and port
 * configured. If creation fails, the <code>LocateRegistry.getRegistry</code>
 * method is called to get a remote instance of the registry. Note, that
 * <code>getRegistry</code> does not create an actual registry on the given
 * host/port nor does it check, whether an RMI registry is active.
 * <p>
 * When the registry has been retrieved, either by creation or by just creating
 * a remote instance, the repository is bound to the configured name in the
 * registry.
 * <p>
 * Possible causes for registration failures include:
 * <ul>
 * <li>The web application is not configured to register with an RMI registry at
 * all.
 * <li>The registry is expected to be running on a remote host but does not.
 * <li>The registry is expected to be running on the local host but cannot be
 * accessed. Reasons include another application which does not act as an
 * RMI registry is running on the configured port and thus blocks creation
 * of a new RMI registry.
 * <li>An object may already be bound to the same name as is configured to be
 * used for the repository.
 * </ul>
 * <p>
 * <b>Note:</b> if a <code>bootstrap-config</code> init parameter is specified the
 * servlet tries to read the respective resource, either as context resource or
 * as file. The properties specified in this file override the init params
 * specified in the <code>web.xml</code>.
 * <p>
 * <p>
 * <b>Setup Wizard Functionality</b><br>
 * When using the first time, the configuraition can miss the relevant
 * repository parameters in the web.xml. if so, it must contain a
 * <code>bootstrap-config</code> parameter that referrs to a propertiy file.
 * This file must exsit for proper working. If not, the repository is not
 * started.<br>
 * If the servlet is not configured correctly and accessed via http, it will
 * provide a simple wizard for the first time configuration. It propmpts for
 * a new (or existing) repository home and will copy the templates of the
 * repository.xml and bootstrap.properties to the respective location.
 */
public class RepositoryStartupServlet extends AbstractRepositoryServlet {

    /**
     * the default logger
     */
    private static final Logger log = LoggerFactory.getLogger(RepositoryStartupServlet.class);

    /**
     * the context attribute name foe 'this' instance.
     */
    private final static String CTX_PARAM_THIS = "repository.startup.servet";

    /**
     * initial param name for the bootstrap config location
     */
    public final static String INIT_PARAM_BOOTSTRAP_CONFIG = "bootstrap-config";

    /**
     * Ugly hack to override the bootstrap file location in the test cases
     */
    static String bootstrapOverride = null;

    /**
     * the registered repository
     */
    private Repository repository;

    /**
     * the jndi context; created based on configuration
     */
    private InitialContext jndiContext;

    private Registry rmiRegistry = null;

    /**
     * Keeps a strong reference to the server side RMI repository instance to
     * prevent the RMI distributed Garbage Collector from collecting the
     * instance making the repository unaccessible though it should still be.
     * This field is only set to a non-<code>null</code> value, if registration
     * of the repository to an RMI registry succeeded in the
     * {@link #registerRMI()} method.
     *
     * @see #registerRMI()
     * @see #unregisterRMI()
     */
    private Remote rmiRepository;

    /**
     * the file to the bootstrap config
     */
    private File bootstrapConfigFile;

    /**
     * The bootstrap configuration
     */
    private BootstrapConfig config;

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
            throw new ServletException("Only one repository startup servlet allowed per web-app.");
        }
        getServletContext().setAttribute(CTX_PARAM_THIS, this);
        startup();
    }

    /**
     * Returns an instance of this servlet. Please note, that only 1
     * repository startup servlet can exist per webapp.
     *
     * @param context the servlet context
     * @return this servlet
     */
    public static RepositoryStartupServlet getInstance(ServletContext context) {
        return (RepositoryStartupServlet) context.getAttribute(CTX_PARAM_THIS);
    }

    /**
     * Configures and starts the repository. It registers it then to the
     * RMI registry and bind is to the JNDI context if so configured.
     * @throws ServletException if an error occurs.
     */
    public void startup() throws ServletException {
        if (repository != null) {
            log.error("Startup: Repository already running.");
            throw new ServletException("Repository already running.");
        }
        log.info("RepositoryStartupServlet initializing...");
        try {
            if (configure()) {
                initRepository();
                registerRMI();
                registerJNDI();
            }

            //Once repository is initialized get its instances bounded to ServletContext
            //via super class init
            if (repository != null){
                super.init();
            }

            log.info("RepositoryStartupServlet initialized.");
        } catch (ServletException e) {
            // shutdown repository
            shutdownRepository();
            log.error("RepositoryStartupServlet initializing failed: " + e, e);
        }
    }

    /**
     * Does a shutdown of the repository and deregisters it from the RMI
     * registry and unbinds if from the JNDI context if so configured.
     */
    public void shutdown() {
        if (repository == null) {
            log.info("Shutdown: Repository already stopped.");
        } else {
            log.info("RepositoryStartupServlet shutting down...");
            unregisterOSGi();
            shutdownRepository();
            unregisterRMI();
            unregisterJNDI();
            log.info("RepositoryStartupServlet shut down.");
        }
    }

    /**
     * Restarts the repository.
     * @throws ServletException if an error occurs.
     * @see #shutdown()
     * @see #startup()
     */
    public void restart() throws ServletException {
        if (repository != null) {
            shutdown();
        }
        startup();
    }

    /**
     * destroy the servlet
     */
    public void destroy() {
        super.destroy();
        shutdown();
    }

    /**
     * Returns the started repository or <code>null</code> if not started
     * yet.
     * @return the JCR repository
     */
    public Repository getRepository() {
        return repository;
    }

    /**
     * Returns a repository factory that returns the repository if available
     * or throws an exception if not.
     *
     * @return repository factory
     */
    public RepositoryFactory getRepositoryFactory() {
        return new RepositoryFactory() {
            public Repository getRepository() throws RepositoryException {
                Repository r = repository;
                if (r != null) {
                    return repository;
                } else {
                    throw new RepositoryException("Repository not available");
                }
            }
        };
    }

    /**
     * Reads the configuration and initializes the {@link #config} field if
     * successful.
     * @throws ServletException if an error occurs.
     */
    private boolean configure() throws ServletException {
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
                bootstrapConfigFile = new File(bstrp);
                if (bootstrapConfigFile.canRead()) {
                    try {
                        in = new FileInputStream(bootstrapConfigFile);
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
                    throw new ServletException(
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
        config = new BootstrapConfig();
        config.init(getServletConfig());
        config.init(bootstrapProps);
        config.validate();
        if (!config.isValid()
                || config.getRepositoryHome() == null) {
            if (bstrp == null) {
                log.error("Repository startup configuration is not valid.");
            } else {
                log.error("Repository startup configuration is not valid but a bootstrap config is specified.");
                log.error("Either create the {} file or", bstrp);
                log.error("use the '/config/index.jsp' for easy configuration.");
            }
            return false;
        } else {
            config.logInfos();
            return true;
        }
    }

    /**
     * Creates a new Repository based on the configuration and initializes the
     * {@link #repository} field if successful.
     *
     * @throws ServletException if an error occurs
     */
    private void initRepository() throws ServletException {
        // get repository config
        File repHome;
        try {
            repHome = new File(config.getRepositoryHome()).getCanonicalFile();
        } catch (IOException e) {
            throw new ServletExceptionWithCause(
                    "Repository configuration failure: " + config.getRepositoryHome(), e);
        }
        String repConfig = config.getRepositoryConfig();
        if (repConfig != null) {
            File configJson = new File(repHome, repConfig);
            if (!configJson.exists()){
                InputStream in = getServletContext().getResourceAsStream(repConfig);
                if (in == null){
                    throw new ServletException("No config file found in classpath " + repConfig);
                }
                OutputStream os = null;
                try {
                    os = FileUtils.openOutputStream(configJson);
                    IOUtils.copy(in, os);
                } catch (IOException e1) {
                    throw new ServletExceptionWithCause(
                            "Error copying the repository config json", e1);
                } finally {
                    IOUtils.closeQuietly(os);
                    IOUtils.closeQuietly(in);
                }
            }

            try {
                repository = createRepository(configJson, repHome);
                if (getBootstrapConfig().isRepositoryCreateDefaultIndexes()){
                    new IndexInitializer(repository).initialize();
                }
            } catch (RepositoryException e) {
                throw new ServletExceptionWithCause("Error while creating repository", e);
            }
        }
    }

    /**
     * Shuts down the repository. If the repository is an instanceof
     * {@link JackrabbitRepository} it's {@link JackrabbitRepository#shutdown()}
     * method is called. in any case, the {@link #repository} field is
     * <code>nulled</code>.
     */
    private void shutdownRepository() {
        if (repository instanceof JackrabbitRepository) {
            ((JackrabbitRepository) repository).shutdown();
        }
        repository = null;
    }

    /**
     * Creates the repository instance for the given config and homedir.
     * Subclasses may override this method of providing own implementations of
     * a {@link Repository}.
     *
     * @param configJson input source of the repository config
     * @param homedir the repository home directory
     * @return a new jcr repository.
     * @throws RepositoryException if an error during creation occurs.
     */
    protected Repository createRepository(File configJson, File homedir)
            throws RepositoryException {
        Map<String,Object> config = new HashMap<String, Object>();
        config.put(OakOSGiRepositoryFactory.REPOSITORY_HOME, homedir.getAbsolutePath());
        config.put(OakOSGiRepositoryFactory.REPOSITORY_CONFIG_FILE, configJson.getAbsolutePath());
        config.put(OakOSGiRepositoryFactory.REPOSITORY_BUNDLE_FILTER, getBootstrapConfig().getBundleFilter());
        config.put(OakOSGiRepositoryFactory.REPOSITORY_SHUTDOWN_ON_TIMEOUT, getBootstrapConfig().isShutdownOnTimeout());
        config.put(OakOSGiRepositoryFactory.REPOSITORY_TIMEOUT_IN_SECS, getBootstrapConfig().getStartupTimeout());
        configureActivator(config);
        //TODO oak-jcr also provides a dummy RepositoryFactory. Hence this
        //cannot be used
        //return JcrUtils.getRepository(config);
        Repository repository = new OakOSGiRepositoryFactory().getRepository(config);
        configWebConsoleSecurityProvider(repository);
        return repository;
    }

    private void configWebConsoleSecurityProvider(Repository repository) {
        if (repository instanceof ServiceRegistryProvider){
            PojoServiceRegistry registry = ((ServiceRegistryProvider) repository).getServiceRegistry();
            registry.registerService(WebConsoleSecurityProvider.class.getName(),
                    new RepositorySecurityProvider(repository), null);
        }
    }

    private void configureActivator(Map<String, Object> config) {
        try{
            config.put(BundleActivator.class.getName(), new BundleActivator() {
                @Override
                public void start(BundleContext bundleContext) throws Exception {
                    registerOSGi(bundleContext);
                }

                @Override
                public void stop(BundleContext bundleContext) throws Exception {
                    unregisterOSGi();
                }
            });
        } catch (Throwable t){
            log.warn("OSGi support not present", t);
        }
    }

    /**
     * Binds the repository to the JNDI context
     * @throws ServletException if an error occurs.
     */
    private void registerJNDI() throws ServletException {
        JNDIConfig jc = config.getJndiConfig();
        if (jc.isValid() && jc.enabled()) {
            try {
                jndiContext = new InitialContext(jc.getJndiEnv());
                jndiContext.bind(jc.getJndiName(), repository);
                log.info("Repository bound to JNDI with name: " + jc.getJndiName());
            } catch (NamingException e) {
                throw new ServletExceptionWithCause(
                        "Unable to bind repository using JNDI: " + jc.getJndiName(), e);
            }
        }
    }

    /**
     * Unbinds the repository from the JNDI context.
     */
    private void unregisterJNDI() {
        if (jndiContext != null) {
            try {
                jndiContext.unbind(config.getJndiConfig().getJndiName());
            } catch (NamingException e) {
                log("Error while unbinding repository from JNDI: " + e);
            }
        }
    }

    /**
     * Registers the repository to an RMI registry configured in the web
     * application. See <a href="#registerAlgo">Registration with RMI</a> in the
     * class documentation for a description of the algorithms used to register
     * the repository with an RMI registry.
     * @throws ServletException if an error occurs.
     */
    private void registerRMI() {
        RMIConfig rc = config.getRmiConfig();
        if (!rc.isValid() || !rc.enabled()) {
            return;
        }

        // try to create remote repository
        Remote remote;
        try {
            Class<?> clazz = Class.forName(getRemoteFactoryDelegaterClass());
            RemoteFactoryDelegater rmf = (RemoteFactoryDelegater) clazz.newInstance();
            remote = rmf.createRemoteRepository(repository);
        } catch (RemoteException e) {
            log.warn("Unable to create RMI repository.", e);
            return;
        } catch (Throwable t) {
            log.warn("Unable to create RMI repository."
                    + " The jcr-rmi jar might be missing.", t);
            return;
        }

        try {
            System.setProperty("java.rmi.server.useCodebaseOnly", "true");
            Registry reg = null;

            // first try to create the registry, which will fail if another
            // application is already running on the configured host/port
            // or if the rmiHost is not local
            try {
                // find the server socket factory: use the default if the
                // rmiHost is not configured
                RMIServerSocketFactory sf;
                if (rc.getRmiHost().length() > 0) {
                    log.debug("Creating RMIServerSocketFactory for host " + rc.getRmiHost());
                    InetAddress hostAddress = InetAddress.getByName(rc.getRmiHost());
                    sf = getRMIServerSocketFactory(hostAddress);
                } else {
                    // have the RMI implementation decide which factory is the
                    // default actually
                    log.debug("Using default RMIServerSocketFactory");
                    sf = null;
                }

                // create a registry using the default client socket factory
                // and the server socket factory retrieved above. This also
                // binds to the server socket to the rmiHost:rmiPort.
                reg = LocateRegistry.createRegistry(rc.rmiPort(), null, sf);
                rmiRegistry = reg;
            } catch (UnknownHostException uhe) {
                // thrown if the rmiHost cannot be resolved into an IP-Address
                // by getRMIServerSocketFactory
                log.info("Cannot create Registry", uhe);
            } catch (RemoteException e) {
                // thrown by createRegistry if binding to the rmiHost:rmiPort
                // fails, for example due to rmiHost being remote or another
                // application already being bound to the port
                log.info("Cannot create Registry", e);
            }

            // if creation of the registry failed, we try to access an
            // potentially active registry. We do not check yet, whether the
            // registry is actually accessible.
            if (reg == null) {
                log.debug("Trying to access existing registry at " + rc.getRmiHost()
                        + ":" + rc.getRmiPort());
                try {
                    reg = LocateRegistry.getRegistry(rc.getRmiHost(), rc.rmiPort());
                } catch (RemoteException re) {
                    log.warn("Cannot create the reference to the registry at "
                            + rc.getRmiHost() + ":" + rc.getRmiPort(), re);
                }
            }

            // if we finally have a registry, register the repository with the
            // rmiName
            if (reg != null) {
                log.debug("Registering repository as " + rc.getRmiName()
                        + " to registry " + reg);
                reg.bind(rc.getRmiName(), remote);

                // when successfull, keep references
                this.rmiRepository = remote;
                log.info("Repository bound via RMI with name: " + rc.getRmiUri());
            } else {
                log.info("RMI registry missing, cannot bind repository via RMI");
            }
        } catch (RemoteException e) {
            log.warn("Unable to bind repository via RMI: " + rc.getRmiUri(), e);
        } catch (AlreadyBoundException e) {
            log.warn("Unable to bind repository via RMI: " + rc.getRmiUri(), e);
        }
    }

    /**
     * Unregisters the repository from the RMI registry, if it has previously
     * been registered.
     */
    private void unregisterRMI() {
        if (rmiRepository != null) {
            // Forcibly unexport the repository;
            try {
                UnicastRemoteObject.unexportObject(rmiRepository, true);
            } catch (NoSuchObjectException e) {
                log.warn("Odd, the RMI repository was not exported", e);
            }
            // drop strong reference to remote repository
            rmiRepository = null;

            // unregister repository
            try {
                Naming.unbind(config.getRmiConfig().getRmiUri());
            } catch (Exception e) {
                log("Error while unbinding repository from JNDI: " + e);
            }
        }

        if (rmiRegistry != null) {
            try {
                UnicastRemoteObject.unexportObject(rmiRegistry, true);
            } catch (NoSuchObjectException e) {
                log.warn("Odd, the RMI registry was not exported", e);
            }
            rmiRegistry = null;
        }
    }

    /**
     * Set the BundleContext reference with ServletContext. This is then used by
     * Felix Proxy Servlet. Kept the type as object to allow logic to work in
     * absence of OSGi classes also.
     * @param bundleContext
     */
    private void registerOSGi(Object bundleContext) {
        getServletContext().setAttribute("org.osgi.framework.BundleContext", bundleContext);
    }

    private void unregisterOSGi() {
        getServletContext().removeAttribute("org.osgi.framework.BundleContext");
    }

    /**
     * Returns the config that was used to bootstrap this servlet.
     * @return the bootstrap config or <code>null</code>.
     */
    public BootstrapConfig getBootstrapConfig() {
        return config;
    }

    /**
     * Return the fully qualified name of the class providing the remote
     * repository. The class whose name is returned must implement the
     * {@link RemoteFactoryDelegater} interface.
     * <p>
     * Subclasses may override this method for providing a name of a own
     * implementation.
     *
     * @return getClass().getName() + "$RMIRemoteFactoryDelegater"
     */
    protected String getRemoteFactoryDelegaterClass() {
        return getClass().getName() + "$RMIRemoteFactoryDelegater";
    }

    /**
     * Returns an <code>RMIServerSocketFactory</code> used to create the server
     * socket for a locally created RMI registry.
     * <p>
     * This implementation returns a new instance of a simple
     * <code>RMIServerSocketFactory</code> which just creates instances of
     * the <code>java.net.ServerSocket</code> class bound to the given
     * <code>hostAddress</code>. Implementations may overwrite this method to
     * provide factory instances, which provide more elaborate server socket
     * creation, such as SSL server sockets.
     *
     * @param hostAddress The <code>InetAddress</code> instance representing the
     *                    the interface on the local host to which the server sockets are
     *                    bound.
     * @return A new instance of a simple <code>RMIServerSocketFactory</code>
     *         creating <code>java.net.ServerSocket</code> instances bound to
     *         the <code>rmiHost</code>.
     */
    protected RMIServerSocketFactory getRMIServerSocketFactory(
            final InetAddress hostAddress) {
        return new RMIServerSocketFactory() {
            public ServerSocket createServerSocket(int port) throws IOException {
                return new ServerSocket(port, -1, hostAddress);
            }
        };
    }

    /**
     * optional class for RMI, will only be used, if RMI server is present
     */
    protected static abstract class RemoteFactoryDelegater {

        public abstract Remote createRemoteRepository(Repository repository)
                throws RemoteException;
    }

    /**
     * optional class for RMI, will only be used, if RMI server is present
     */
    protected static class RMIRemoteFactoryDelegater extends RemoteFactoryDelegater {

        private static final RemoteAdapterFactory FACTORY =
            new ServerAdapterFactory();

        public Remote createRemoteRepository(Repository repository)
                throws RemoteException {
            return FACTORY.getRemoteRepository(repository);
        }

    }

    //-------------------------------------------------< Installer Routines >---

    /**
     * {@inheritDoc}
     */
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        if (repository == null) {
            redirect(req, resp, "/bootstrap/missing.jsp");
        } else {
            redirect(req, resp, "/bootstrap/running.jsp");
        }
    }

    /**
     * {@inheritDoc}
     */
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        if (repository != null) {
            redirect(req, resp, "/bootstrap/reconfigure.jsp");
        } else {
            int rc = new Installer(bootstrapConfigFile,
                    getServletContext()).installRepository(req);
            switch (rc) {
                case Installer.C_INSTALL_OK:
                    // restart rep
                    restart();
                    if (repository == null) {
                        redirect(req, resp, "/bootstrap/error.jsp");
                    } else {
                        redirect(req, resp, "/bootstrap/success.jsp");
                    }
                    break;
                case Installer.C_INVALID_INPUT:
                    redirect(req, resp, "/bootstrap/missing.jsp");
                    break;
                case Installer.C_CONFIG_EXISTS:
                case Installer.C_BOOTSTRAP_EXISTS:
                case Installer.C_HOME_EXISTS:
                    redirect(req, resp, "/bootstrap/exists.jsp");
                    break;
                case Installer. C_HOME_MISSING:
                case Installer.C_CONFIG_MISSING:
                    redirect(req, resp, "/bootstrap/notexists.jsp");
                    break;
                case Installer.C_INSTALL_ERROR:
                    redirect(req, resp, "/bootstrap/error.jsp");
                    break;
            }
        }
    }

    /**
     * Helper function to send a redirect response respecting the context path.
     *
     * @param req the request
     * @param resp the response
     * @param loc the location for the redirect
     * @throws ServletException if an servlet error occurs.
     * @throws IOException if an I/O error occurs.
     */
    private void redirect(HttpServletRequest req,
                          HttpServletResponse resp, String loc)
            throws ServletException, IOException {
        String cp = req.getContextPath();
        if (cp == null || cp.equals("/")) {
            cp = "";
        }
        resp.sendRedirect(cp + loc);
    }
}

