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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigHelper {
    private static final Logger LOG = LoggerFactory.getLogger(PipelinedStrategy.class);

    public static int getSystemPropertyAsInt(String name, int defaultValue) {
        int result = Integer.getInteger(name, defaultValue);
        LOG.info("Config {}={}", name, result);
        return result;
    }

    public static String getSystemPropertyAsString(String name, String defaultValue) {
        String result = System.getProperty(name, defaultValue);
        LOG.info("Config {}={}", name, result);
        return result;
    }

    public static boolean getSystemPropertyAsBoolean(String name, boolean defaultValue) {
        String sysPropValue = System.getProperty(name);
        boolean value;
        if (sysPropValue == null) {
            value = defaultValue;
        } else {
            value = Boolean.parseBoolean(sysPropValue);
        }

        LOG.info("Config {}={}", name, value);
        return value;
    }
}
