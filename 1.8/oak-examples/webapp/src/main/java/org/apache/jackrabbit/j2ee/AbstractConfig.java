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

import org.apache.commons.collections.BeanMap;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;

/**
 * Abstract configuration class that is based on a bean map.
 */
public abstract class AbstractConfig {

    /**
     * default logger
     */
    private static final Logger log = LoggerFactory.getLogger(AbstractConfig.class);

    protected boolean valid;

    private BeanMap map = new BeanMap(this);

    /**
     * Initializes the configuration with values from the given properties
     * @param props the configuration properties
     */
    public void init(Properties props) throws ServletException {
        Iterator iter = props.keySet().iterator();
        while (iter.hasNext()) {
            String name = (String) iter.next();
            String mapName = toMapName(name, '.');
            try {
                if (map.containsKey(mapName)) {
                    map.put(mapName, props.getProperty(name));
                }
            } catch (Exception e) {
                throw new ServletExceptionWithCause(
                        "Invalid configuration property: " + name, e);
            }
        }
    }

    public void init(ServletConfig ctx) throws ServletException {
        Enumeration names = ctx.getInitParameterNames();
        while (names.hasMoreElements()) {
            String name = (String) names.nextElement();
            String mapName = toMapName(name, '-');
            try {
                if (map.containsKey(mapName)) {
                    map.put(mapName, ctx.getInitParameter(name));
                }
            } catch (Exception e) {
                throw new ServletExceptionWithCause(
                        "Invalid servlet configuration option: " + name, e);
            }
        }
    }

    public String toMapName(String name, char delim) {
        StringBuffer ret = new StringBuffer();
        String[] elems = Text.explode(name, delim);
        ret.append(elems[0]);
        for (int i=1; i<elems.length; i++) {
            ret.append(elems[i].substring(0, 1).toUpperCase());
            ret.append(elems[i].substring(1));
        }
        return ret.toString();
    }

    public void validate() {
        valid = true;
    }

    public boolean isValid() {
        return valid;
    }

    public void logInfos() {
        log.info("Configuration of {}", Text.getName(this.getClass().getName(), '.'));
        log.info("----------------------------------------------");
        Iterator iter = map.keySet().iterator();
        while (iter.hasNext()) {
            String name = (String) iter.next();
            log.info("  {}: {}", name, map.get(name));
        }
        log.info("----------------------------------------------");
    }
}