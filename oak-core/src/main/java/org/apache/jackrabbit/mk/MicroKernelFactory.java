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
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.client.Client;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.mk.fs.FileUtils;
import org.apache.jackrabbit.mk.index.IndexWrapper;
import org.apache.jackrabbit.mk.server.Server;
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
    public static MicroKernel getInstance(String url) {
        int colon = url.indexOf(':');
        if (colon == -1) {
            throw new IllegalArgumentException("Unknown repository URL: " + url);
        }

        String head = url.substring(0, colon);
        String tail = url.substring(colon + 1);
        if (head.equals("mem") || head.equals("simple") || head.equals("fs")) {
            boolean clean = false;
            if (tail.endsWith(";clean")) {
                tail = tail.substring(0, tail.length() - ";clean".length());
                clean = true;
            }

            tail = tail.replaceAll("\\{homeDir\\}", System.getProperty("homeDir", "."));

            if (clean) {
                String dir;
                if (head.equals("fs")) {
                    dir = tail + "/.mk";
                } else {
                    dir = tail.substring(tail.lastIndexOf(':') + 1);
                    INSTANCES.remove(tail);
                }

                try {
                    FileUtils.deleteRecursive(dir, false);
                } catch (Exception e) {
                    throw ExceptionFactory.convert(e);
                }
            }

            if (head.equals("fs")) {
                return new MicroKernelImpl(tail);
            } else {
                final String name = tail;
                synchronized (INSTANCES) {
                    SimpleKernelImpl instance = INSTANCES.get(name);
                    if (instance == null) {
                        instance = new SimpleKernelImpl(name) {
                            @Override
                            public synchronized void dispose() {
                                super.dispose();
                                synchronized (INSTANCES) {
                                    INSTANCES.remove(name);
                                }
                            }
                        };
                        INSTANCES.put(name, instance);
                    }
                    return instance;
                }
            }
        } else if (head.equals("log")) {
            return new LogWrapper(getInstance(tail));
        } else if (head.equals("sec")) {
            return SecurityWrapper.get(url);
        } else if (head.equals("virtual")) {
            MicroKernel mk = getInstance(tail);
            try {
                return new VirtualRepositoryWrapper(mk);
            } catch (MicroKernelException e) {
                mk.dispose();
                throw e;
            }
        } else if (head.equals("index")) {
            return new IndexWrapper(getInstance(tail));
        } else if (head.equals("http")) {
            return new Client(url);
        } else if (head.equals("http-bridge")) {
            MicroKernel mk = getInstance(tail);

            final Server server = new Server(mk);
            try {
                server.start();
            } catch (IOException e) {
                throw new IllegalArgumentException(e.getMessage());
            }

            return new Client(server.getAddress()) {
                @Override
                public synchronized void dispose() {
                    super.dispose();
                    server.stop();
                }
            };
        } else {
            throw new IllegalArgumentException(url);
        }
    }

}
