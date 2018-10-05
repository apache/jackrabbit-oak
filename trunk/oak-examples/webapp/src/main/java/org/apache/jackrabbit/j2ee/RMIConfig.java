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

import java.net.URI;
import java.net.URISyntaxException;
import java.rmi.registry.Registry;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;

/**
 * The RMI config hold information about RMI connection details.
 *
 * It supports the following properties and init parameters:
 * <pre>
 * +-------------------+--------------------+
 * | Property Name     | Init-Param Name    |
 * +-------------------+--------------------+
 * | rmi.enable        | {rmi-port sepc.}   |
 * | rmi.host          | rmi-host           |
 * | rmi.port          | rmi-port           |
 * | rmi.name          | {repository name}  |
 * | rmi.url           | rmi-url            |
 * +-------------------+--------------------+
 * </pre>
 */
public class RMIConfig extends AbstractConfig {

    /**
     * default logger
     */
    private static final Logger log = LoggerFactory.getLogger(RMIConfig.class);

    private boolean rmiEnabled;

    private int rmiPort = -1;

    private String rmiHost;

    private String rmiName;

    private String rmiUri;

    private final BootstrapConfig parentConfig;


    public RMIConfig(BootstrapConfig parentConfig) {
        this.parentConfig = parentConfig;
    }

    public void init(ServletConfig ctx) throws ServletException {
        super.init(ctx);
        // enable RMI if either port or url was defined
        rmiEnabled = rmiPort >=0 || rmiUri != null;
    }

    public String getRmiName() {
        return rmiName;
    }

    public void setRmiName(String rmiName) {
        this.rmiName = rmiName;
    }

    public boolean enabled() {
        return rmiEnabled;
    }

    public String getRmiEnabled() {
        return String.valueOf(rmiEnabled);
    }

    public void setRmiEnabled(String rmiEnabled) {
        this.rmiEnabled = Boolean.valueOf(rmiEnabled).booleanValue();
    }

    public int rmiPort() {
        return rmiPort;
    }

    public String getRmiPort() {
        return String.valueOf(rmiPort);
    }

    public void setRmiPort(String rmiPort) {
        this.rmiPort = Integer.decode(rmiPort).intValue();
    }

    public String getRmiHost() {
        return rmiHost;
    }

    public void setRmiHost(String rmiHost) {
        this.rmiHost = rmiHost;
    }

    public String getRmiUri() {
        return rmiUri;
    }

    public void setRmiUri(String rmiUri) {
        this.rmiUri = rmiUri;
    }

    public void validate() {
        if (!rmiEnabled) {
            return;
        }

        if (rmiUri != null && rmiUri.length() > 0) {
            // URI takes precedences, so check whether the configuration has to
            // be set from the URI
            try {
                URI uri = new URI(rmiUri);

                // extract values from the URI, check later
                rmiHost = uri.getHost();
                rmiPort = uri.getPort();
                rmiName = uri.getPath();

            } catch (URISyntaxException e) {
                log.warn("Cannot parse RMI URI '" + rmiUri + "'.", e);
                rmiUri = null; // clear RMI URI use another one
                rmiHost = null; // use default host, ignore rmi-host param
            }

            // cut of leading slash from name if defined at all
            if (rmiName != null && rmiName.startsWith("/")) {
                rmiName = rmiName.substring(1);
            }
        }

        // check RMI port
        if (rmiPort == -1 || rmiPort == 0) {
            // accept -1 or 0 as a hint to use the default
            rmiPort = Registry.REGISTRY_PORT;
        } else if (rmiPort < -1 || rmiPort > 0xFFFF) {
            // emit a warning if out of range, use defualt in this case
            log.warn("Invalid port in rmi-port param " + rmiPort + ". using default port.");
            rmiPort = Registry.REGISTRY_PORT;
        }

        // check host - use an empty name if null (i.e. not configured)
        if (rmiHost == null) {
            rmiHost = "";
        }

        // check name - use repositoryName if empty or null
        if (rmiName == null || rmiName.length() ==0) {
            rmiName = parentConfig.getRepositoryName();
        }

        // reconstruct the rmiURI now because values might have been changed
        rmiUri = "//" + rmiHost + ":" + rmiPort + "/" + rmiName;
        valid = true;
    }
}