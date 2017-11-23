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
package org.apache.jackrabbit.oak.commons;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.annotation.Nonnull;

/**
 * Provides version information about Oak.
 */
public final class OakVersion {

    private OakVersion() {
    }

    /**
     * Returns the version of an Oak module.
     * 
     * @param moduleName the name of the module
     * @param clazz a class of the module
     * @return the version (or "SNAPSHOT" when unknown)
     */
    @Nonnull
    public static String getVersion(String moduleName, Class clazz) {
        String version = "SNAPSHOT"; // fallback
        InputStream stream = clazz.getResourceAsStream(
                "/META-INF/maven/org.apache.jackrabbit/" +
                        moduleName + "/pom.properties");
        if (stream != null) {
            try {
                try {
                    Properties properties = new Properties();
                    properties.load(stream);
                    version = properties.getProperty("version", version);
                } finally {
                    stream.close();
                }
            } catch (IOException e) {
                // ignore
            }
        }
        return version;
    }
}
