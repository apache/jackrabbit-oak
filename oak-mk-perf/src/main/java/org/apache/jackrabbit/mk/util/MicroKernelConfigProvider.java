/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mk.util;

import java.io.InputStream;
import java.util.Properties;

public class MicroKernelConfigProvider {

    /**
     * Read the mk configuration from file.
     * 
     * @param resourcePath
     * @return
     * @throws Exception
     */
    public static Configuration readConfig(String resourcePath)
            throws Exception {

        InputStream is = MicroKernelConfigProvider.class
                .getResourceAsStream(resourcePath);

        Properties properties = new Properties();
        properties.load(is);
        is.close();
        return new Configuration(properties);
    }

    /**
     * Read the mk configuration from config.cfg.
     * 
     * @param resourcePath
     * @return
     * @throws Exception
     */
    public static Configuration readConfig() throws Exception {

        InputStream is = MicroKernelConfigProvider.class
                .getResourceAsStream("/config.cfg");

        Properties properties = new Properties();
        properties.load(is);
        // System.out.println(properties.toString());
        is.close();
        return new Configuration(properties);
    }

}
