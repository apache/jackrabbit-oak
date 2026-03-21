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
package org.apache.jackrabbit.core.data;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.jackrabbit.core.data.util.NamedThreadFactory;

/**
 * Abstract Backend which has a reference to the underlying {@link CachingDataStore} and is
 * maintaining the lifecycle of the internal asynchronous write executor.
 */
public abstract class AbstractBackend implements Backend {

    /**
     * {@link CachingDataStore} instance using this backend.
     */
    private CachingDataStore dataStore;

    /**
     * path of repository home dir.
     */
    private String homeDir;

    /**
     * path of config property file.
     */
    private String config;

    /**
     * The pool size of asynchronous write pooling executor.
     */
    private int asyncWritePoolSize = 10;

    /**
     * Asynchronous write pooling executor.
     */
    private volatile Executor asyncWriteExecutor;

    /**
     * Returns the pool size of the asynchronous write pool executor.
     * @return the pool size of the asynchronous write pool executor
     */
    public int getAsyncWritePoolSize() {
        return asyncWritePoolSize;
    }

    /**
     * Sets the pool size of the asynchronous write pool executor.
     * @param asyncWritePoolSize pool size of the async write pool executor
     */
    public void setAsyncWritePoolSize(int asyncWritePoolSize) {
        this.asyncWritePoolSize = asyncWritePoolSize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(CachingDataStore dataStore, String homeDir, String config) throws DataStoreException {
        this.dataStore = dataStore;
        this.homeDir = homeDir;
        this.config = config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws DataStoreException {
        Executor asyncExecutor = getAsyncWriteExecutor();

        if (asyncExecutor != null && asyncExecutor instanceof ExecutorService) {
            ((ExecutorService) asyncExecutor).shutdownNow();
        }
    }

    /**
     * Returns the {@link CachingDataStore} instance using this backend.
     * @return the {@link CachingDataStore} instance using this backend
     */
    protected CachingDataStore getDataStore() {
        return dataStore;
    }

    /**
     * Sets the {@link CachingDataStore} instance using this backend.
     * @param dataStore the {@link CachingDataStore} instance using this backend
     */
    protected void setDataStore(CachingDataStore dataStore) {
        this.dataStore = dataStore;
    }

    /**
     * Returns path of repository home dir.
     * @return path of repository home dir
     */
    protected String getHomeDir() {
        return homeDir;
    }

    /**
     * Sets path of repository home dir.
     * @param homeDir path of repository home dir
     */
    protected void setHomeDir(String homeDir) {
        this.homeDir = homeDir;
    }

    /**
     * Returns path of config property file.
     * @return path of config property file
     */
    protected String getConfig() {
        return config;
    }

    /**
     * Sets path of config property file.
     * @param config path of config property file
     */
    protected void setConfig(String config) {
        this.config = config;
    }

    /**
     * Returns Executor used to execute asynchronous write or touch jobs.
     * @return Executor used to execute asynchronous write or touch jobs
     */
    protected Executor getAsyncWriteExecutor() {
        Executor executor = asyncWriteExecutor;

        if (executor == null) {
            synchronized (this) {
                executor = asyncWriteExecutor;
                if (executor == null) {
                    asyncWriteExecutor = executor = createAsyncWriteExecutor();
                }
            }
        }

        return executor;
    }

    /**
     * Creates an {@link Executor}.
     * This method is invoked during the initialization for asynchronous write/touch job executions.
     * @return an {@link Executor}
     */
    protected Executor createAsyncWriteExecutor() {
        Executor asyncExecutor;

        if (dataStore.getAsyncUploadLimit() > 0 && getAsyncWritePoolSize() > 0) {
            asyncExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(getAsyncWritePoolSize(),
                    new NamedThreadFactory(getClass().getSimpleName() + "-write-worker"));
        } else {
            asyncExecutor = new ImmediateExecutor();
        }

        return asyncExecutor;
    }

    /**
     * This class implements {@link Executor} interface to run {@code command} right away,
     * resulting in non-asynchronous mode executions.
     */
    private class ImmediateExecutor implements Executor {
        @Override
        public void execute(Runnable command) {
            command.run();
        }
    }

}
