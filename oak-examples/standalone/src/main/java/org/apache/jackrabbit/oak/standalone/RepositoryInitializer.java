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

package org.apache.jackrabbit.oak.standalone;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.servlet.ServletContext;

import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.felix.connect.launch.PojoServiceRegistry;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory;
import org.apache.jackrabbit.oak.run.osgi.ServiceRegistryProvider;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

@Configuration
public class RepositoryInitializer {
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Autowired
    private ServletContext servletContext;

    private Repository repository;

    @Value("classpath:/repository-config.json")
    private Resource defaultRepoConfig;

    @Value("${repo.home}")
    private String repoHome;

    @PostConstruct
    public void initialize() throws Exception {
        initRepository();
    }

    @PreDestroy
    public void close() {
        if (repository instanceof JackrabbitRepository) {
            ((JackrabbitRepository) repository).shutdown();
            log.info("Repository shutdown complete");
            repository = null;
        }
    }

    @Bean(name="repository")
    public Repository getRepository(){
        return repository;
    }

    @Bean
    public PojoServiceRegistry getServiceRegistry(){
        return ((ServiceRegistryProvider) repository).getServiceRegistry();
    }

    private void initRepository() throws IOException, RepositoryException {
        File repoHomeDir = new File(repoHome);
        FileUtils.forceMkdir(repoHomeDir);

        File repoConfig = new File(repoHomeDir, "repository-config.json");
        copyDefaultConfig(repoConfig, defaultRepoConfig);
        repository = createRepository(repoConfig, repoHomeDir);
    }

    private Repository createRepository(File repoConfig, File repoHomeDir) throws RepositoryException {
        Map<String,Object> config = Maps.newHashMap();
        config.put(OakOSGiRepositoryFactory.REPOSITORY_HOME, repoHomeDir.getAbsolutePath());
        config.put(OakOSGiRepositoryFactory.REPOSITORY_CONFIG_FILE, repoConfig.getAbsolutePath());
        config.put(OakOSGiRepositoryFactory.REPOSITORY_SHUTDOWN_ON_TIMEOUT, false);
        config.put(OakOSGiRepositoryFactory.REPOSITORY_ENV_SPRING_BOOT, true);
        config.put(OakOSGiRepositoryFactory.REPOSITORY_TIMEOUT_IN_SECS, 10);

        config.put("repo.home", repoHomeDir.getAbsolutePath());

        configureActivator(config);
        return new OakOSGiRepositoryFactory().getRepository(config);
    }

    private void configureActivator(Map<String, Object> config) {
        config.put(BundleActivator.class.getName(), new BundleActivator() {
            @Override
            public void start(BundleContext bundleContext) throws Exception {
                servletContext.setAttribute(BundleContext.class.getName(), bundleContext);
            }

            @Override
            public void stop(BundleContext bundleContext) throws Exception {
                servletContext.removeAttribute(BundleContext.class.getName());
            }
        });

    }

    private void copyDefaultConfig(File repoConfig, Resource defaultRepoConfig)
            throws IOException, RepositoryException {
        if (!repoConfig.exists()){
            log.info("Copying default repository config to {}", repoConfig.getAbsolutePath());
            InputStream in = defaultRepoConfig.getInputStream();
            if (in == null){
                throw new RepositoryException("No config file found in classpath " + defaultRepoConfig);
            }
            OutputStream os = null;
            try {
                os = FileUtils.openOutputStream(repoConfig);
                IOUtils.copy(in, os);
            } finally {
                IOUtils.closeQuietly(os);
                IOUtils.closeQuietly(in);
            }
        }
    }


}
