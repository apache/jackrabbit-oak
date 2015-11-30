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
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.servlet.ServletContext;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
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
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

@Configuration
public class RepositoryInitializer {
    public static final String ARG_MONGO = "mongo";
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Autowired
    private ServletContext servletContext;

    private Repository repository;

    @Value("classpath:/repository-config.json")
    private Resource defaultRepoConfig;

    @Value("${repo.home}")
    private String repoHome;

    @Value("${oak.mongo.db}")
    private String mongoDbName;

    @Value("${oak.mongo.uri}")
    private String mongouri;

    @Autowired
    private ApplicationArguments args;

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

        List<String> configFileNames = determineConfigFileNamesToCopy();
        List<String> configFilePaths = copyConfigs(repoHomeDir, configFileNames);
        repository = createRepository(configFilePaths, repoHomeDir);
    }

    private Repository createRepository(List<String> repoConfigs, File repoHomeDir) throws RepositoryException {
        Map<String,Object> config = Maps.newHashMap();
        config.put(OakOSGiRepositoryFactory.REPOSITORY_HOME, repoHomeDir.getAbsolutePath());
        config.put(OakOSGiRepositoryFactory.REPOSITORY_CONFIG_FILE, commaSepFilePaths(repoConfigs));
        config.put(OakOSGiRepositoryFactory.REPOSITORY_SHUTDOWN_ON_TIMEOUT, false);
        config.put(OakOSGiRepositoryFactory.REPOSITORY_ENV_SPRING_BOOT, true);
        config.put(OakOSGiRepositoryFactory.REPOSITORY_TIMEOUT_IN_SECS, 10);

        //Set of properties used to perform property substitution in
        //OSGi configs
        config.put("repo.home", repoHomeDir.getAbsolutePath());
        config.put("oak.mongo.db", mongoDbName);
        config.put("oak.mongo.uri", getMongoURI());

        //Configures BundleActivator to get notified of
        //OSGi startup and shutdown
        configureActivator(config);

        return new OakOSGiRepositoryFactory().getRepository(config);
    }

    private String getMongoURI() {
        List<String> mongoOpts = args.getOptionValues(ARG_MONGO);
        if (mongoOpts != null && !mongoOpts.isEmpty()){
            return mongoOpts.get(0);
        }
        return mongouri;
    }

    private Object commaSepFilePaths(List<String> repoConfigs) {
        return Joiner.on(",").join(repoConfigs);
    }

    private List<String> copyConfigs(File repoHomeDir, List<String> configFileNames)
            throws IOException, RepositoryException {
        List<String> filePaths = Lists.newArrayList();
        for (String configName : configFileNames) {
            File dest = new File(repoHomeDir, configName);
            Resource source = new ClassPathResource("config-templates/" + configName);
            copyDefaultConfig(dest, source);
            filePaths.add(dest.getAbsolutePath());
        }
        return filePaths;
    }


    private List<String> determineConfigFileNamesToCopy() {
        List<String> configNames = Lists.newArrayList();
        configNames.add("repository-config.json");

        //Mongo mode can be selected via --mongo
        if (args.containsOption(ARG_MONGO)) {
            configNames.add("mongomk-config.json");
            log.info("Using Mongo persistence");
        } else {
            configNames.add("segmentmk-config.json");
        }
        return configNames;
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
}
