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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

/**
 * Provides very basic installation capabilities.
 */
public class Installer {

    /**
     * the default logger
     */
    private static final Logger log = LoggerFactory.getLogger(Installer.class);

    /**
     * Return code for installation succeeded
     */
    public static final int C_INSTALL_OK = 0;

    /**
     * Return code for invalid input parameter
     */
    public static final int C_INVALID_INPUT = 1;

    /**
     * Return code for repository home already exists
     */
    public static final int C_HOME_EXISTS = 2;

    /**
     * Return code for repository home is missing
     */
    public static final int C_HOME_MISSING = 3;

    /**
     * Return code for repository config already exists
     */
    public static final int C_CONFIG_EXISTS = 4;

    /**
     * Return code for repository config is missing
     */
    public static final int C_CONFIG_MISSING = 5;

    /**
     * Return code for bootstrap config already exists
     */
    public static final int C_BOOTSTRAP_EXISTS = 6;

    /**
     * Return code for a general install error
     */
    public static final int C_INSTALL_ERROR = 7;

    /**
     * place to store the config file
     */
    private final File bootstrapConfigFile;

    /**
     * the servlet context
     */
    private final ServletContext context;

    /**
     * the place for the repository config template
     * todo: to be configured
     */
    private final String configTemplate =
            "/WEB-INF/templates/repository-config.json";

    /**
     * the place for the bootstrap properties template
     * todo: to be configured
     */
    private final String bootstrapTemplate = "/WEB-INF/templates/bootstrap.properties";

    /**
     * Creates a new installer
     * @param bootstrapConfigFile the location for the config file
     * @param context the servlet context for accessing resources
     */
    public Installer(File bootstrapConfigFile, ServletContext context) {
        this.bootstrapConfigFile = bootstrapConfigFile;
        this.context = context;
    }

    /**
     * Handles the installation.
     *
     * @param req the servlet request with the input parameters
     * @return the installation return code
     *
     * @throws ServletException if a servlet error occurs.
     * @throws IOException if an I/O error occurs.
     */
    public int installRepository(HttpServletRequest req)
            throws ServletException, IOException {
        String repHome = req.getParameter("repository_home");
        String mode = req.getParameter("mode");

        if (repHome == null || mode == null) {
            return C_INVALID_INPUT;
        }
        File home = new File(repHome);
        File config = new File(home, "repository-config.json");

        if ("new".equals(mode)) {
            // Test internal folder repository existence and not home because home is already created
            // by org.apache.jackrabbit.server.remoting.davex.JcrRemotingServlet
            if (config.exists()) {
                log.error("Trying to install new repository config '{}' but already exists", config);
                return C_CONFIG_EXISTS;
            }
            log.info("Creating new repository home '{}'", repHome);
            home.mkdirs();

            try {
                installRepositoryConfig(config);
            } catch (IOException e) {
                log.error("Error while installing new repository config '{}': {}", config, e.toString());
                return C_BOOTSTRAP_EXISTS;
            }
        } else {
            if (!home.exists()) {
                log.error("Trying to use existing repository home '{}' but does not exists", repHome);
                return C_HOME_MISSING;
            }
            if (!config.exists()) {
                log.error("Trying to use existing repository config '{}' but does not exists", config);
                return C_CONFIG_MISSING;
            }
        }
        // install bootstrap.properties
        try {
            installBootstrap(bootstrapConfigFile, repHome);
        } catch (IOException e) {
            log.error("Error while installing '{}': {}", bootstrapConfigFile.getPath(), e.toString());
            return C_INSTALL_ERROR;
        }
        return C_INSTALL_OK;
    }

    /**
     * Installs the repository config file from the template
     * @param dest the destination location
     * @throws IOException if an I/O error occurs.
     */
    private void installRepositoryConfig(File dest) throws IOException {
        log.info("Creating new repository config: {}", dest.getPath());
        InputStream in = context.getResourceAsStream(configTemplate);
        if (in == null) {
            in = getClass().getResourceAsStream(configTemplate);
        }
        OutputStream out = new FileOutputStream(dest);
        byte[] buffer = new byte[8192];
        int read;
        while ((read = in.read(buffer)) >= 0) {
            out.write(buffer, 0, read);
        }
        in.close();
        out.close();
    }

    /**
     * Installs the bootstrap config file from the template
     * @param dest the destination location
     * @param repHome the repository home location
     * @throws IOException if an I/O error occurs
     */
    private void installBootstrap(File dest, String repHome)
            throws IOException {
        log.info("Creating new bootstrap properties: {}", dest.getPath());
        InputStream in = context.getResourceAsStream(bootstrapTemplate);
        Properties props = new Properties();
        props.load(in);
        props.setProperty("repository.home", repHome);
        in.close();
        if (!dest.getParentFile().exists()) {
            dest.getParentFile().mkdirs();
        }
        OutputStream out = new FileOutputStream(dest);
        props.store(out, "bootstrap properties for the repository startup servlet.");
        out.close();
    }

}
