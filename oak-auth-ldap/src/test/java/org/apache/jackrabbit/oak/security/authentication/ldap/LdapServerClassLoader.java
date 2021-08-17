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

package org.apache.jackrabbit.oak.security.authentication.ldap;

import com.google.common.io.ByteStreams;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.core.Appender;

import org.apache.directory.server.ldap.LdapServer;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.ProtectionDomain;
import java.util.Collection;

/**
 * The LDAP server we use for testing relies on an old, incompatible version of the library
 * <code>org.apache.directory.api.api-all</code>. Therefore we have to run it in it's own classloader, because the two
 * incompatible versions of the library must not live on the same classpath.
 */
public class LdapServerClassLoader extends URLClassLoader {

    private final byte[] serverClassResource;
    private final byte[] serverBaseClassResource;

    private LdapServerClassLoader(URL[] urls, Class serverClass, Class serverBaseClass) throws IOException {
        super(urls, ClassLoader.getSystemClassLoader().getParent());
        this.serverClassResource = ByteStreams.toByteArray(
                serverClass.getResourceAsStream("/".concat(serverClass.getCanonicalName()).replace('.', '/').concat(".class")));
        this.serverBaseClassResource = ByteStreams.toByteArray(
                serverBaseClass.getResourceAsStream("/".concat(serverBaseClass.getCanonicalName()).replace('.', '/').concat(".class")));
    }

    public static LdapServerClassLoader createServerClassLoader() throws URISyntaxException, ClassNotFoundException, IOException {
        ClassLoader appClassLoader = LdapServerClassLoader.class.getClassLoader();
        String apacheDsUrl = appClassLoader.getResource(
                LdapServer.class.getCanonicalName().replace(".", "/").concat(".class"))
                .toURI()
                .getRawSchemeSpecificPart();
        apacheDsUrl = apacheDsUrl.substring(0, apacheDsUrl.lastIndexOf('!'));

        // also add URL classloader for Logback Classic (SLF4J Impl) ...
        String logbackClassicUrl = appClassLoader.getResource(
                Logger.class.getCanonicalName().replace(".", "/").concat(".class"))
                .toURI()
                .getRawSchemeSpecificPart();
        logbackClassicUrl = logbackClassicUrl.substring(0, logbackClassicUrl.lastIndexOf('!'));

        // ... its transitive dependency Logback Classic ...
        String logbackCoreUrl = appClassLoader.getResource(
                Appender.class.getCanonicalName().replace(".", "/").concat(".class"))
                .toURI()
                .getRawSchemeSpecificPart();
        logbackCoreUrl = logbackCoreUrl.substring(0, logbackCoreUrl.lastIndexOf('!'));

        // ... and the configuration folder containing the logback-test.xml
        String configFolderUrl = appClassLoader.getResource("logback-test.xml").toString();
        configFolderUrl = configFolderUrl.substring(0, configFolderUrl.lastIndexOf('/') + 1);

        Class<?> sc = appClassLoader.loadClass(InternalLdapServer.class.getCanonicalName());
        Class<?> sbc = appClassLoader.loadClass(AbstractServer.class.getCanonicalName());
        return new LdapServerClassLoader(new URL[] { 
                new URI(apacheDsUrl).toURL(),  
                new URI(logbackClassicUrl).toURL(), 
                new URI(logbackCoreUrl).toURL(),
                new URI(configFolderUrl).toURL() }, 
                sc, sbc);
    }

    public Proxy createAndSetupServer() throws Exception {
        return createAndSetupServer(false);
    }
    
    public Proxy createAndSetupServer(boolean useSSL) throws Exception {
        final Proxy proxy = new Proxy();
        final Exception[] ex = new Exception[] { null };
        Runnable r = () -> {
            try {
                proxy.serverClass = loadClass(InternalLdapServer.class.getCanonicalName());
                Constructor<?> constructor = proxy.serverClass.getConstructor(Boolean.TYPE);
                proxy.server = constructor.newInstance(useSSL);
                proxy.serverClass.getMethod("setUp", new Class[0]).invoke(proxy.server);
                proxy.port = (int) proxy.serverClass.getMethod("getPort", new Class[0]).invoke(proxy.server);
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                ex[0] = e;
            }
        };
        Thread t = new Thread(r);
        t.setContextClassLoader(this);
        t.start();
        t.join();
        if (ex[0] != null) {
            throw ex[0];
        }
        return proxy;
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        if (InternalLdapServer.class.getCanonicalName().equals(name)) {
            return defineClass(name, serverClassResource, 0, serverClassResource.length, (ProtectionDomain) null);
        }
        if (AbstractServer.class.getCanonicalName().equals(name)) {
            return defineClass(name, serverBaseClassResource, 0, serverBaseClassResource.length, (ProtectionDomain) null);
        }
        return super.findClass(name);
    }

    public static class Proxy {

        //Proxy class for InternalLdapServer, using the correct ClassLoader. If marshalling of complex types is
        //involved in a method call, a new thread with the correct context ClassLoader will execute the call to
        //avoid casting issues (objects of identical types might not be castable across ClassLoaders).

        public static Class serverClass;
        public Object server;
        public int port;
        public String host = "127.0.0.1";

        public void tearDown() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
            serverClass.getMethod("tearDown", new Class[0]).invoke(server);
        }

        public void setMaxSizeLimit(long limit) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
            serverClass.getMethod("setMaxSizeLimit", new Class[] { Long.TYPE }).invoke(server, limit);
        }

        public void loadLdif(InputStream in) throws Exception {
            final Exception[] ex = new Exception[] { null };
            Runnable r = () -> {
                try {
                    serverClass.getMethod("loadLdif", new Class[] {InputStream.class}).invoke(server, in);
                } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                    ex[0] = e;
                }

            };
            Thread t = new Thread(r);
            t.setContextClassLoader(serverClass.getClassLoader());
            t.start();
            t.join();
            if (ex[0] != null) {
                throw ex[0];
            }
        }

        public String addUser(String firstName, String lastName, String userId, String password) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
            return (String) serverClass.getMethod("addUser", new Class[] {String.class, String.class, String.class, String.class}).invoke(server, firstName, lastName, userId, password);
        }

        public String addGroup(String name, String member) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
            return (String) serverClass.getMethod("addGroup", new Class[] {String.class, String.class}).invoke(server, name, member);
        }

        public void addMember(String groupDN, String memberDN) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
            serverClass.getMethod("addMember", new Class[] {String.class, String.class}).invoke(server, groupDN, memberDN);
        }

        public void addMembers(String name, Collection<String> members) throws Exception {
            final Exception[] ex = new Exception[] { null };
            Runnable r = () -> {
                try {
                    serverClass.getMethod("addMembers", new Class[] {String.class, Collection.class}).invoke(server, name, members);
                } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                    ex[0] = e;
                }

            };
            Thread t = new Thread(r);
            t.setContextClassLoader(serverClass.getClassLoader());
            t.start();
            t.join();
            if (ex[0] != null) {
                throw ex[0];
            }
        }
    }
}
