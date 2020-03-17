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

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletException;
import javax.servlet.ServletConfig;

/**
 * The bootstrap configuration hold information about initial startup
 * parameters like repository config and home.
 *
 * It supports the following properties and init parameters:
 * <pre>
 * +-------------------+-------------------+
 * | Property Name     | Init-Param Name   |
 * +-------------------+-------------------+
 * | repository.home   | repository-home   |
 * | repository.config | repository-config |
 * | repository.name   | repository-name   |
 * +-------------------+-------------------+
 * </pre>
 */
public class BootstrapConfig extends AbstractConfig {

    private String repositoryHome;

    private String repositoryConfig;

    private String repositoryName;

    private String bundleFilter;

    //By default shutdown framework if there is a timeout
    private boolean shutdownOnTimeout = true;

    private boolean repositoryCreateDefaultIndexes = true;

    private int startupTimeout = (int) TimeUnit.MINUTES.toSeconds(5); //Default 5 minute timeout

    private JNDIConfig jndiConfig = new JNDIConfig(this);

    private RMIConfig rmiConfig = new RMIConfig(this);

    public void init(Properties props) throws ServletException {
        super.init(props);
        jndiConfig.init(props);
        rmiConfig.init(props);
    }

    public void init(ServletConfig ctx) throws ServletException {
        super.init(ctx);
        jndiConfig.init(ctx);
        rmiConfig.init(ctx);
    }

    public String getRepositoryHome() {
        return repositoryHome;
    }

    public void setRepositoryHome(String repositoryHome) {
        this.repositoryHome = repositoryHome;
    }

    public String getRepositoryConfig() {
        return repositoryConfig;
    }

    public void setRepositoryConfig(String repositoryConfig) {
        this.repositoryConfig = repositoryConfig;
    }

    public String getRepositoryName() {
        return repositoryName;
    }

    public void setRepositoryName(String repositoryName) {
        this.repositoryName = repositoryName;
    }

    public String getBundleFilter() {
        return bundleFilter;
    }

    public void setBundleFilter(String bundleFilter) {
        this.bundleFilter = bundleFilter;
    }

    public JNDIConfig getJndiConfig() {
        return jndiConfig;
    }

    public RMIConfig getRmiConfig() {
        return rmiConfig;
    }

    public boolean isShutdownOnTimeout() {
        return shutdownOnTimeout;
    }

    public void setShutdownOnTimeout(boolean shutdownOnTimeout) {
        this.shutdownOnTimeout = shutdownOnTimeout;
    }

    public int getStartupTimeout() {
        return startupTimeout;
    }

    public void setStartupTimeout(int startupTimeout) {
        this.startupTimeout = startupTimeout;
    }

    public boolean isRepositoryCreateDefaultIndexes() {
        return repositoryCreateDefaultIndexes;
    }

    public void setRepositoryCreateDefaultIndexes(boolean repositoryCreateDefaultIndexes) {
        this.repositoryCreateDefaultIndexes = repositoryCreateDefaultIndexes;
    }

    public void validate() {
        valid = repositoryName != null;
        jndiConfig.validate();
        rmiConfig.validate();
    }


    public void logInfos() {
        super.logInfos();
        if (jndiConfig.isValid()) {
            jndiConfig.logInfos();
        }
        if (rmiConfig.isValid()) {
            rmiConfig.logInfos();
        }
    }
}