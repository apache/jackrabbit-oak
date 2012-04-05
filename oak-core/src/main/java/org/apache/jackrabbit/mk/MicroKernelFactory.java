/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mk;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.client.Client;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.mk.fs.FileUtils;
import org.apache.jackrabbit.mk.index.IndexWrapper;
import org.apache.jackrabbit.mk.simple.SimpleKernelImpl;
import org.apache.jackrabbit.mk.util.ExceptionFactory;
import org.apache.jackrabbit.mk.wrapper.LogWrapper;
import org.apache.jackrabbit.mk.wrapper.SecurityWrapper;
import org.apache.jackrabbit.mk.wrapper.VirtualRepositoryWrapper;

/**
 * A factory to create a MicroKernel instance.
 */
public class MicroKernelFactory {

    private static final Map<String, SimpleKernelImpl> INSTANCES =
            new HashMap<String, SimpleKernelImpl>();

    /**
     * Get an instance. Supported URLs:
     * <ul>
     * <li>fs:target/mk-test (using the directory ./target/mk-test)</li>
     * <li>fs:target/mk-test;clean (same, but delete the old repository first)</li>
     * <li>fs:{homeDir} (use the system property homeDir or '.' if not set)</li>
     * <li>simple: (in-memory implementation)</li>
     * <li>simple:fs:target/temp (using the directory ./target/temp)</li>
     * </ul>
     *
     * @param url the repository URL
     * @return a new instance
     */
    public static synchronized MicroKernel getInstance(String url) {
        if (url.startsWith("mem:") || url.startsWith("simple:")) {
            boolean clean = false;
            if (url.endsWith(";clean")) {
                url = url.substring(0, url.length() - ";clean".length());
                clean = true;
            }

            url = url.replaceAll("\\{homeDir\\}", System.getProperty("homeDir", "."));

            String name;
            if (url.startsWith("simple:")) {
                name = url.substring("simple:".length());
            } else {
                name = url.substring("mem:".length());
            }

            if (clean) {
                String dir = url.substring(url.lastIndexOf(':') + 1);
                try {
                    FileUtils.deleteRecursive(dir, false);
                } catch (Exception e) {
                    throw ExceptionFactory.convert(e);
                }
                INSTANCES.remove(name);
            }

            SimpleKernelImpl instance = INSTANCES.get(name);
            if (instance == null) {
                instance = new SimpleKernelImpl(name);
                INSTANCES.put(name, instance);
            }
            return instance;
        } else if (url.startsWith("log:")) {
            return LogWrapper.get(url);
        } else if (url.startsWith("sec:")) {
            return SecurityWrapper.get(url);
        } else if (url.startsWith("virtual:")) {
            return VirtualRepositoryWrapper.get(url);
        } else if (url.startsWith("index:")) {
            return IndexWrapper.get(url);
        } else if (url.startsWith("fs:")) {
            boolean clean = false;
            if (url.endsWith(";clean")) {
                url = url.substring(0, url.length() - ";clean".length());
                clean = true;
            }
            String dir = url.substring("fs:".length());
            dir = dir.replaceAll("\\{homeDir\\}", System.getProperty("homeDir", "."));
            if (clean) {
                try {
                    FileUtils.deleteRecursive(dir + "/" + ".mk", false);
                } catch (IOException e) {
                    throw ExceptionFactory.convert(e);
                }
            }
            return new MicroKernelImpl(dir);
        } else if (url.startsWith("http:")) {
            return Client.createHttpClient(url);
        } else if (url.startsWith("http-bridge:")) {
            MicroKernel mk = MicroKernelFactory.getInstance(url.substring("http-bridge:".length()));
            return Client.createHttpBridge(mk);
        } else {
            throw new IllegalArgumentException(url);
        }
    }

}
