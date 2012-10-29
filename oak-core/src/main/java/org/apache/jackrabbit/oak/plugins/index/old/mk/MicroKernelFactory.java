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
package org.apache.jackrabbit.oak.plugins.index.old.mk;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.client.Client;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.mk.server.Server;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.SimpleKernelImpl;
import org.apache.jackrabbit.oak.plugins.index.old.mk.wrapper.LogWrapper;
import org.apache.jackrabbit.oak.plugins.index.old.mk.wrapper.SecurityWrapper;
import org.apache.jackrabbit.oak.plugins.index.old.mk.wrapper.VirtualRepositoryWrapper;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A factory to create a MicroKernel instance.
 */
public class MicroKernelFactory {

    private static final Map<String, SimpleKernelImpl> INSTANCES =
            new HashMap<String, SimpleKernelImpl>();

    private MicroKernelFactory() {
    }

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
                // TODO: The factory should not control repository lifecycle
                // See also https://issues.apache.org/jira/browse/OAK-32
                String dir;
                if (head.equals("fs")) {
                    dir = tail + "/.mk";
                } else {
                    dir = tail.substring(tail.lastIndexOf(':') + 1);
                    INSTANCES.remove(tail);
                }
                deleteRecursive(new File(dir));
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
            String userPassUrl = tail;
            int index = userPassUrl.indexOf(':');
            if (index < 0) {
                throw ExceptionFactory.get("Expected url format: sec:user@pass:<url>");
            }
            String u = userPassUrl.substring(index + 1);
            String userPass = userPassUrl.substring(0, index);
            index = userPass.indexOf('@');
            if (index < 0) {
                throw ExceptionFactory.get("Expected url format: sec:user@pass:<url>");
            }
            String user = userPass.substring(0, index);
            String pass = userPass.substring(index + 1);
            final MicroKernel mk = getInstance(u);
            try {
                return new SecurityWrapper(mk, user, pass) {
                    @Override
                    public void dispose() {
                        super.dispose();
                        MicroKernelFactory.disposeInstance(mk);
                    }
                };
            } catch (MicroKernelException e) {
                MicroKernelFactory.disposeInstance(mk);
                throw e;
            }
        } else if (head.equals("virtual")) {
            final MicroKernel mk = getInstance(tail);
            try {
                return new VirtualRepositoryWrapper(mk) {
                    @Override
                    public void dispose() {
                        super.dispose();
                        MicroKernelFactory.disposeInstance(mk);
                    }
                };
            } catch (MicroKernelException e) {
                MicroKernelFactory.disposeInstance(mk);
                throw e;
            }
        } else if (head.equals("index")) {
            final MicroKernel mk = getInstance(tail);
            try {
                return new IndexWrapper(mk) {
                    public void dispose() {
                        MicroKernelFactory.disposeInstance(mk);
                    }
                };
            } catch (MicroKernelException e) {
                MicroKernelFactory.disposeInstance(mk);
                throw e;
            }
        } else if (head.equals("http")) {
            return new Client(url);
        } else if (head.equals("http-bridge")) {
            final MicroKernel mk = getInstance(tail);

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
                    MicroKernelFactory.disposeInstance(mk);
                }
            };
        } else {
            throw new IllegalArgumentException(url);
        }
    }

    /**
     * Disposes an instance that was created by this factory.
     * @param mk MicroKernel instance
     */
    public static void disposeInstance(MicroKernel mk) {
        if (mk instanceof MicroKernelImpl) {
            ((MicroKernelImpl) mk).dispose();
        } else if (mk instanceof SimpleKernelImpl) {
            ((SimpleKernelImpl) mk).dispose();
        } else if (mk instanceof Client) {
            ((Client) mk).dispose();
        } else if (mk instanceof LogWrapper) {
            ((LogWrapper) mk).dispose();
        } else if (mk instanceof SecurityWrapper) {
            ((SecurityWrapper) mk).dispose();
        } else if (mk instanceof VirtualRepositoryWrapper) {
            ((VirtualRepositoryWrapper) mk).dispose();
        } else if (mk instanceof IndexWrapper) {
            ((IndexWrapper) mk).dispose();
        } else {
            throw new IllegalArgumentException("instance was not created by this factory");
        }
    }

    /**
    * Delete a directory or file and all subdirectories and files inside it.
    *
    * @param file the file denoting the directory to delete
    */
    private static void deleteRecursive(File file) {
        File[] files = file.listFiles();
        for (int i = 0; files != null && i < files.length; i++) {
            deleteRecursive(files[i]);
        }
        file.delete();
    }
}
