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

import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;

/**
 * The JNDI config hold information about JNDI connection details.
 *
 * It supports the following properties and init parameters:
 * <pre>
 * +-------------------+--------------------+
 * | Property Name     | Init-Param Name    |
 * +-------------------+--------------------+
 * | jndi.enable       | {provider spec.}   |
 * | java.naming.*     | java.naming.*      |
 * +-------------------+--------------------+
 * </pre>
 */
public class JNDIConfig extends AbstractConfig {

    private boolean jndiEnabled;

    private String jndiName;

    private final BootstrapConfig parentConfig;

    private Properties jndiEnv = new Properties();


    public JNDIConfig(BootstrapConfig parentConfig) {
        this.parentConfig = parentConfig;
    }


    public String getJndiName() {
        return jndiName;
    }

    public void setJndiName(String jndiName) {
        this.jndiName = jndiName;
    }

    public boolean enabled() {
        return jndiEnabled;
    }

    public String getJndiEnabled() {
        return String.valueOf(jndiEnabled);
    }

    public void setJndiEnabled(String jndiEnabled) {
        this.jndiEnabled = Boolean.valueOf(jndiEnabled).booleanValue();
    }

    public Properties getJndiEnv() {
        return jndiEnv;
    }

    public void init(Properties props) throws ServletException {
        super.init(props);
        // add all props whose name starts with 'java.namming.' to the env
        Iterator iter = props.keySet().iterator();
        while (iter.hasNext()) {
            String name = (String) iter.next();
            if (name.startsWith("java.naming.")) {
                jndiEnv.put(name, props.getProperty(name));
            }
        }
    }

    public void init(ServletConfig ctx) throws ServletException  {
        super.init(ctx);
        // add all params whose name starts with 'java.namming.' to the env
        Enumeration names = ctx.getInitParameterNames();
        while (names.hasMoreElements()) {
            String name = (String) names.nextElement();
            if (name.startsWith("java.naming.")) {
                jndiEnv.put(name, ctx.getInitParameter(name));
            }
        }
        // enable jndi if url is specified
        jndiEnabled = jndiEnv.containsKey("java.naming.provider.url");
    }


    public void validate() {
        if (jndiName == null) {
            jndiName = parentConfig.getRepositoryName();
        }
        valid = true;
    }
}