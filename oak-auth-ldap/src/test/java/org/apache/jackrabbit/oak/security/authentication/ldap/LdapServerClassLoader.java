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

        Class<?> sc = appClassLoader.loadClass(InternalLdapServer.class.getCanonicalName());
        Class<?> sbc = appClassLoader.loadClass(AbstractServer.class.getCanonicalName());
        return new LdapServerClassLoader(new URL[] { new URI(apacheDsUrl).toURL() }, sc, sbc);
    }

    public Proxy createAndSetupServer() throws Exception {
        final Proxy proxy = new Proxy();
        final Exception[] ex = new Exception[] { null };
        Runnable r = () -> {
            try {
                proxy.serverClass = loadClass(InternalLdapServer.class.getCanonicalName());
                Constructor<?> constructor = proxy.serverClass.getConstructor(new Class[0]);
                proxy.server = constructor.newInstance(new Object[0]);
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

        public void addMembers(String name, Iterable<String> members) throws Exception {
            final Exception[] ex = new Exception[] { null };
            Runnable r = () -> {
                try {
                    serverClass.getMethod("addMembers", new Class[] {String.class, Iterable.class}).invoke(server, name, members);
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
