/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.run.osgi;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.felix.connect.Revision;
import org.apache.felix.connect.launch.BundleDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For running PojoSR based application in Spring Boot environment we need to provide
 * a custom Revision implementation. Default PojoSR JarRevision support works for normal
 * jar files while in Spring Boot env jar files are embedded withing another jar.
 *
 * This class does not have direct dependency on Spring Boot Jar Launcher but relies
 * on reflection as the Spring Jar support is not visible to PojoSR classloader
 */
class SpringBootSupport {
    private static Logger log = LoggerFactory.getLogger(SpringBootSupport.class);

    public static final String SPRING_BOOT_PACKAGE = "org.springframework.boot.loader.jar";

    public static List<BundleDescriptor> processDescriptors(List<BundleDescriptor> descriptors)
            throws IOException {
        List<BundleDescriptor> processed = Lists.newArrayList();
        for (BundleDescriptor desc : descriptors) {
            if (desc.getRevision() == null) {
                URL u = new URL(desc.getUrl());
                URLConnection uc = u.openConnection();
                if (uc instanceof JarURLConnection
                        && uc.getClass().getName().startsWith(SPRING_BOOT_PACKAGE)) {
                    Revision rev = new SpringBootJarRevision(((JarURLConnection) uc).getJarFile(),
                            uc.getLastModified());
                    desc = new BundleDescriptor(desc.getClassLoader(), desc.getUrl(), desc.getHeaders(),
                            rev, desc.getServices());
                }
            }
            processed.add(desc);
        }
        return processed;
    }

    /**
     * Key change here is use of org.springframework.boot.loader.jar.JarEntry.getUrl()
     * to get a working URL which allows access to files present in embedded jars
     */
    private static class SpringBootJarRevision implements Revision {
        private static Method ENTRY_URL_METHOD;
        private final JarFile jarFile;
        private final long lastModified;

        private SpringBootJarRevision(JarFile jarFile, long lastModified) {
            this.jarFile = jarFile;
            //Taken from org.apache.felix.connect.JarRevision
            if (lastModified > 0) {
                this.lastModified = lastModified;
            } else {
                this.lastModified = System.currentTimeMillis();
            }
        }

        @Override
        public long getLastModified() {
            return lastModified;
        }

        @Override
        public URL getEntry(String entryName) {
            try {
                JarEntry jarEntry = jarFile.getJarEntry(entryName);
                /*
                  JarEntry here is instance of org.springframework.boot.loader.jar.JarEntry
                  However as that class is loaded in different classloader and not visible
                  actual invocation has to be done via reflection. URL returned here has proper
                  Handler configured to allow reverse access via URL connection
                 */
                if (jarEntry != null) {
                    return (URL) getUrlMethod(jarEntry).invoke(jarEntry);
                }
            } catch (Exception e) {
                log.warn("Error occurred while fetching jar entry {} from {}", entryName, jarFile, e);
            }
            return null;
        }

        @Override
        public Enumeration<String> getEntries() {
            final Enumeration<JarEntry> e = jarFile.entries();
            return Iterators.asEnumeration(new AbstractIterator<String>() {
                @Override
                protected String computeNext() {
                    if (e.hasMoreElements()){
                        return e.nextElement().getName();
                    }
                    return endOfData();
                }
            });
        }

        private static Method getUrlMethod(JarEntry jarEntry) throws NoSuchMethodException {
            if (ENTRY_URL_METHOD == null){
                Preconditions.checkState(jarEntry.getClass().getName().startsWith(SPRING_BOOT_PACKAGE),
                        "JarEntry class %s does not belong to Spring package", jarEntry.getClass());
                ENTRY_URL_METHOD = jarEntry.getClass().getMethod("getUrl");
            }
            return ENTRY_URL_METHOD;
        }
    }
}
