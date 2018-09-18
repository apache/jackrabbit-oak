/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *  
 *    http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License. 
 *  
 */
package org.apache.jackrabbit.oak.security.authentication.ldap;


import static org.junit.Assume.assumeFalse;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.BindException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;

import org.apache.commons.io.FileUtils;
import org.apache.directory.api.ldap.model.constants.SupportedSaslMechanisms;
import org.apache.directory.api.ldap.model.entry.DefaultEntry;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.exception.LdapConfigurationException;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.ldif.LdifEntry;
import org.apache.directory.api.ldap.model.ldif.LdifReader;
import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.directory.api.ldap.model.schema.SchemaManager;
import org.apache.directory.api.ldap.schema.manager.impl.DefaultSchemaManager;
import org.apache.directory.server.constants.ServerDNConstants;
import org.apache.directory.server.constants.SystemSchemaConstants;
import org.apache.directory.server.core.DefaultDirectoryService;
import org.apache.directory.server.core.api.CacheService;
import org.apache.directory.server.core.api.CoreSession;
import org.apache.directory.server.core.api.DirectoryService;
import org.apache.directory.server.core.api.InstanceLayout;
import org.apache.directory.server.core.api.schema.SchemaPartition;
import org.apache.directory.server.core.jndi.CoreContextFactory;
import org.apache.directory.server.core.partition.impl.avl.AvlPartition;
import org.apache.directory.server.core.shared.DefaultDnFactory;
import org.apache.directory.server.ldap.LdapServer;
import org.apache.directory.server.ldap.handlers.extended.StartTlsHandler;
import org.apache.directory.server.ldap.handlers.extended.StoredProcedureExtendedOperationHandler;
import org.apache.directory.server.ldap.handlers.sasl.MechanismHandler;
import org.apache.directory.server.ldap.handlers.sasl.cramMD5.CramMd5MechanismHandler;
import org.apache.directory.server.ldap.handlers.sasl.digestMD5.DigestMd5MechanismHandler;
import org.apache.directory.server.ldap.handlers.sasl.gssapi.GssapiMechanismHandler;
import org.apache.directory.server.ldap.handlers.sasl.ntlm.NtlmMechanismHandler;
import org.apache.directory.server.ldap.handlers.sasl.plain.PlainMechanismHandler;
import org.apache.directory.server.protocol.shared.transport.TcpTransport;
import org.apache.mina.util.AvailablePortFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple ldap test server
 */
public abstract class AbstractServer {

    public static final String  EXAMPLE_DN = "dc=example,dc=com";

    private static final Logger LOG = LoggerFactory.getLogger(AbstractServer.class);
    private static final List<LdifEntry> EMPTY_LIST = Collections.unmodifiableList(new ArrayList<LdifEntry>(0));
    private static final String CTX_FACTORY = "com.sun.jndi.ldap.LdapCtxFactory";

    /**
     * the context root for the rootDSE
     */
    protected CoreSession rootDSE;

    /**
     * flag whether to delete database files for each test or not
     */
    protected boolean doDelete = true;

    protected int port = -1;

    protected CacheService cacheService;

    protected DirectoryService directoryService;

    protected LdapServer ldapServer;


    /**
     * Loads an LDIF from an input stream and adds the entries it contains to
     * the server.  It appears as though the administrator added these entries
     * to the server.
     *
     * @param in            the input stream containing the LDIF entries to load
     * @return a list of entries added to the server in the order they were added
     * @throws NamingException of the load fails
     */
    protected List<LdifEntry> loadLdif(InputStream in) throws Exception {
        if (in == null) {
            return EMPTY_LIST;
        }
        LdifReader ldifReader = new LdifReader(in);
        return loadLdif(ldifReader);
    }

    protected List<LdifEntry> loadLdif(LdifReader ldifReader) throws Exception {
        List<LdifEntry> entries = new ArrayList<LdifEntry>();
        for (LdifEntry ldifEntry : ldifReader) {
            Dn dn = ldifEntry.getDn();
            if (ldifEntry.isEntry()) {
                org.apache.directory.api.ldap.model.entry.Entry items = ldifEntry.getEntry();
                rootDSE.add(new DefaultEntry(directoryService.getSchemaManager(), items));
                LOG.info("Added entry {}", dn);
                entries.add(ldifEntry);
            }
        }
        return entries;
    }

    /**
     * Inject an ldif String into the server. DN must be relative to the
     * root.
     *
     * @param ldif the entries to inject
     * @throws NamingException if the entries cannot be added
     */
    protected void addEntry(String ldif) throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(ldif.getBytes("utf-8"));
        LdifReader reader = new LdifReader(in);
        loadLdif(reader);
    }

    /**
     * Common code to get an initial context via a simple bind to the
     * server over the wire using the SUN JNDI LDAP provider. Do not use
     * this method until after the setUp() method is called to start the
     * server otherwise it will fail.
     *
     * @return an LDAP context as the the administrator to the rootDSE
     * @throws NamingException if the server cannot be contacted
     */
    protected LdapContext getWiredContext() throws Exception {
        return getWiredContext(ServerDNConstants.ADMIN_SYSTEM_DN, "secret");
    }


    /**
     * Common code to get an initial context via a simple bind to the
     * server over the wire using the SUN JNDI LDAP provider. Do not use
     * this method until after the setUp() method is called to start the
     * server otherwise it will fail.
     *
     * @param bindPrincipalDn the DN of the principal to bind as
     * @param password        the password of the bind principal
     * @return an LDAP context as the the administrator to the rootDSE
     * @throws NamingException if the server cannot be contacted
     */
    protected LdapContext getWiredContext(String bindPrincipalDn, String password) throws Exception {
        Hashtable<String, String> env = new Hashtable<String, String>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, CTX_FACTORY);
        env.put(Context.PROVIDER_URL, "ldap://localhost:" + port);
        env.put(Context.SECURITY_PRINCIPAL, bindPrincipalDn);
        env.put(Context.SECURITY_CREDENTIALS, password);
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        return new InitialLdapContext(env, null);
    }


    /**
     * Get's the initial context factory for the provider's ou=system context
     * root.
     */
    protected void setUp() throws Exception {
        File cwd = new File("target", "apacheds");
        doDelete(cwd);

        // setup directory service
        directoryService = new DefaultDirectoryService();
        directoryService.setShutdownHookEnabled(false);
        directoryService.setInstanceLayout(new InstanceLayout(cwd));

        cacheService = new CacheService();
        cacheService.initialize(directoryService.getInstanceLayout());

        SchemaManager schemaManager = new DefaultSchemaManager();
        directoryService.setSchemaManager(schemaManager);
        directoryService.setDnFactory(new DefaultDnFactory(directoryService.getSchemaManager(), cacheService.getCache("dnCache")));

        AvlPartition schLdifPart = new AvlPartition(directoryService.getSchemaManager(), directoryService.getDnFactory());
        schLdifPart.setId("schema");
        schLdifPart.setSuffixDn(directoryService.getDnFactory().create(ServerDNConstants.CN_SCHEMA_DN));
        SchemaPartition schPart = new SchemaPartition(directoryService.getSchemaManager());
        schPart.setWrappedPartition(schLdifPart);
        directoryService.setSchemaPartition(schPart);


        AvlPartition sysPart = new AvlPartition(directoryService.getSchemaManager(), directoryService.getDnFactory());
        sysPart.setId(SystemSchemaConstants.SCHEMA_NAME);
        sysPart.setSuffixDn(directoryService.getDnFactory().create(ServerDNConstants.SYSTEM_DN));
        directoryService.setSystemPartition(sysPart);

        AvlPartition examplePart = new AvlPartition(directoryService.getSchemaManager(), directoryService.getDnFactory());
        examplePart.setId("example");
        examplePart.setSuffixDn(directoryService.getDnFactory().create(EXAMPLE_DN));
        examplePart.setCacheService(cacheService);
        directoryService.addPartition(examplePart);

        // setup ldap server
        port = AvailablePortFinder.getNextAvailable(1024);
        ldapServer = new LdapServer();
        setupLdapServer();
        setupSaslMechanisms();

        directoryService.startup();
        setupExamplePartition();
        startLdapServer();
        setContexts(ServerDNConstants.ADMIN_SYSTEM_DN, "secret");
    }

    /**
     * Start the LDAP server assuming we can bind to the previously reserved port.
     * Given that there is a small race between when the port was reserved and when the
     * socket is actually bound this can still fail. For now we are ignoring this rare
     * case and skip the test. See OAK-5542.
     * TODO: OAK-5832: Make the LDAP server used in testing resilient against ports already in use
     * @throws Exception
     */
    private void startLdapServer() throws Exception {
        try {
            ldapServer.start();
        } catch (LdapConfigurationException e) {
            Throwable cause = e.getCause();
            assumeFalse("Ignoring this test as the server port is already in use (OAK-5542): " + cause,
                    cause instanceof BindException);
            throw e;
        }
    }

    protected void setupLdapServer() throws Exception {
        ldapServer.setTransports(new TcpTransport(port));
        ldapServer.setDirectoryService(directoryService);
        ldapServer.addExtendedOperationHandler(new StartTlsHandler());
        ldapServer.addExtendedOperationHandler(new StoredProcedureExtendedOperationHandler());
    }

    protected void setupExamplePartition() throws Exception {
        // setup acl to allow read all users
        // Inject the context entry for dc=example,dc=com partition if it does not already exist
        try {
            directoryService.getAdminSession().lookup(new Dn(EXAMPLE_DN));
        } catch (LdapException lnnfe) {
            Entry entry = directoryService.newEntry(new Dn(EXAMPLE_DN));
            entry.add("objectClass", "top", "domain", "extensibleObject");
            entry.add("dc", "example");
            entry.add("administrativeRole", "accessControlSpecificArea");
            directoryService.getAdminSession().add(entry);

            entry = directoryService.newEntry(new Dn("cn=enableSearchForAllUsers," + EXAMPLE_DN));
            entry.add("objectClass", "top", "subentry", "accessControlSubentry");
            entry.add("cn", "enableSearchForAllUsers");
            entry.add("subtreeSpecification", "{}");
            entry.add("prescriptiveACI",
                    "{ \n" +
                            "  identificationTag \"enableSearchForAllUsers\",\n" +
                            "  precedence 14,\n" +
                            "  authenticationLevel simple,\n" +
                            "  itemOrUserFirst userFirst: \n" +
                            "  { \n" +
                            "    userClasses { allUsers }, \n" +
                            "    userPermissions \n" +
                            "    { \n" +
                            "      {\n" +
                            "        protectedItems {entry, allUserAttributeTypesAndValues}, \n" +
                            "        grantsAndDenials { grantRead, grantReturnDN, grantBrowse } \n" +
                            "      }\n" +
                            "    } \n" +
                            "  } \n" +
                            "}");
            directoryService.getAdminSession().add(entry);
            directoryService.sync();
        }
    }

    public void setMaxSizeLimit(long maxSizeLimit) {
        ldapServer.setMaxSizeLimit(maxSizeLimit);
    }

    private void setupSaslMechanisms() {
        Map<String, MechanismHandler> mechanismHandlerMap = new HashMap<String, MechanismHandler>();

        mechanismHandlerMap.put(SupportedSaslMechanisms.PLAIN, new PlainMechanismHandler());

        CramMd5MechanismHandler cramMd5MechanismHandler = new CramMd5MechanismHandler();
        mechanismHandlerMap.put(SupportedSaslMechanisms.CRAM_MD5, cramMd5MechanismHandler);

        DigestMd5MechanismHandler digestMd5MechanismHandler = new DigestMd5MechanismHandler();
        mechanismHandlerMap.put(SupportedSaslMechanisms.DIGEST_MD5, digestMd5MechanismHandler);

        GssapiMechanismHandler gssapiMechanismHandler = new GssapiMechanismHandler();
        mechanismHandlerMap.put(SupportedSaslMechanisms.GSSAPI, gssapiMechanismHandler);

        NtlmMechanismHandler ntlmMechanismHandler = new NtlmMechanismHandler();
        // TODO - set some sort of default NtlmProvider implementation here
        // ntlmMechanismHandler.setNtlmProvider( provider );
        // TODO - or set FQCN of some sort of default NtlmProvider implementation here
        // ntlmMechanismHandler.setNtlmProviderFqcn( "com.foo.BarNtlmProvider" );
        mechanismHandlerMap.put(SupportedSaslMechanisms.NTLM, ntlmMechanismHandler);
        mechanismHandlerMap.put(SupportedSaslMechanisms.GSS_SPNEGO, ntlmMechanismHandler);

        ldapServer.setSaslMechanismHandlers(mechanismHandlerMap);
    }


    /**
     * Deletes the Eve working directory.
     *
     * @param wkdir the directory to delete
     * @throws IOException if the directory cannot be deleted
     */
    protected void doDelete(File wkdir) throws IOException {
        if (doDelete) {
            if (wkdir.exists()) {
                FileUtils.deleteDirectory(wkdir);
            }

            if (wkdir.exists()) {
                throw new IOException("Failed to delete: " + wkdir);
            }
        }
    }


    /**
     * Sets the contexts for this base class.  Values of user and password used to
     * set the respective JNDI properties.  These values can be overriden by the
     * overrides properties.
     *
     * @param user   the username for authenticating as this user
     * @param passwd the password of the user
     * @throws NamingException if there is a failure of any kind
     */
    protected void setContexts(String user, String passwd) throws Exception {
        Hashtable<String, Object> env = new Hashtable<String, Object>();
        env.put(DirectoryService.JNDI_KEY, directoryService);
        env.put(Context.SECURITY_PRINCIPAL, user);
        env.put(Context.SECURITY_CREDENTIALS, passwd);
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.INITIAL_CONTEXT_FACTORY, CoreContextFactory.class.getName());
        setContexts(env);
    }

    /**
     * Sets the contexts of this class taking into account the extras and overrides
     * properties.
     *
     * @param env an environment to use while setting up the system root.
     * @throws NamingException if there is a failure of any kind
     */
    protected void setContexts(Hashtable<String, Object> env) throws Exception {
        Hashtable<String, Object> envFinal = new Hashtable<String, Object>(env);
        envFinal.put(Context.PROVIDER_URL, "");
        rootDSE = directoryService.getAdminSession();
    }

    /**
     * Sets the system context root to null.
     */
    protected void tearDown() throws Exception {
        if (ldapServer != null) {
            ldapServer.stop();
        }
        try {
            directoryService.shutdown();
        } catch (Exception e) {
            // ignore
        }
        if (cacheService != null) {
            cacheService.destroy();
        }
    }
}
