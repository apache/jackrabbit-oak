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

import javax.jcr.Repository;

import org.apache.jackrabbit.server.remoting.davex.DavexServletService;
import org.apache.jackrabbit.server.remoting.davex.JcrRemotingServlet;
import org.apache.jackrabbit.webdav.simple.SimpleWebdavServlet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.embedded.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configures the Webdav and Davex servlet to enabled remote
 * access to the repository
 */
@Configuration
public class RemotingInitializer {

    @Value("${repo.home}/dav")
    private String davHome;

    @Autowired
    private Repository repository;


    @Bean
    public ServletRegistrationBean webDavServlet() {
        ServletRegistrationBean bean = new ServletRegistrationBean(new SimpleWebdavServlet() {
            @Override
            public Repository getRepository() {
                return repository;
            }
        }, "/repository/*");

        bean.addInitParameter(SimpleWebdavServlet.INIT_PARAM_RESOURCE_PATH_PREFIX, "/repository");
        bean.addInitParameter(SimpleWebdavServlet.INIT_PARAM_RESOURCE_CONFIG, "remoting/webdav-config.xml");
        return bean;
    }

    @Bean
    public ServletRegistrationBean remotingServlet() {
        ServletRegistrationBean bean = new ServletRegistrationBean(new JcrRemotingServlet() {

            @Override
            public Repository getRepository() {
                return repository;
            }
        }, "/server/*");

        bean.addInitParameter(JcrRemotingServlet.INIT_PARAM_RESOURCE_PATH_PREFIX, "/server");
        bean.addInitParameter(JcrRemotingServlet.INIT_PARAM_BATCHREAD_CONFIG, "remoting/batchread.properties");

        //TODO By docs this is meant to point to a file which gets loaded
        //but servlet always reads it as File not via input stream. Hence using
        //actual class
        bean.addInitParameter(JcrRemotingServlet.INIT_PARAM_PROTECTED_HANDLERS_CONFIG,
                "remoting/protectedHandlersConfig.xml");
        bean.addInitParameter(JcrRemotingServlet.INIT_PARAM_HOME, davHome);
        return bean;
    }
}
