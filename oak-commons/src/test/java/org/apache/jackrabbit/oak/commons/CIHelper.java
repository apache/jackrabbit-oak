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
package org.apache.jackrabbit.oak.commons;

import static java.lang.Boolean.parseBoolean;
import static java.lang.System.getenv;

import java.util.Locale;
import java.util.Objects;

/**
 * Utility class for ITs to determine the environment running in.
 */
public final class CIHelper {

    private CIHelper() {
        // Prevent instantiation.
    }

    /**
     * Check if this process is running on Jenkins.
     *
     * @return {@code true} if this process is running on Jenkins, {@code false}
     * otherwise.
     */
    public static boolean jenkins() {
        return getenv("JENKINS_URL") != null;
    }

    /**
     * @return  {@code true} iff running on
     * https://travis-ci.org/
     */
    public static boolean travis() {
        return parseBoolean(getenv("TRAVIS"));
    }

    /**
     * @return  {@code true} iff running on with {@code PROFILE=pedantic}
     * @deprecated Travis builds do not use PROFILE anymore. Use {@link #travis()} instead.
     */
    public static boolean travisPedantic() {
        return Objects.equals(getenv("PROFILE"), "pedantic");
    }

    /**
     * @return  {@code true} iff running on with {@code PROFILE=unittesting}
     * @deprecated Travis builds do not use PROFILE anymore. Use {@link #travis()} instead.
     */
    public static boolean travisUnitTesting() {
        return Objects.equals(getenv("PROFILE"), "unittesting");
    }

    /**
     * @return  {@code true} iff running on with {@code PROFILE=integrationTesting}
     * @deprecated Travis builds do not use PROFILE anymore. Use {@link #travis()} instead.
     */
    public static boolean travisIntegrationTesting() {
        return Objects.equals(getenv("PROFILE"), "integrationTesting");
    }

    public static boolean jenkinsNodeLabel(String label) {
        String labels = getenv("NODE_LABELS");
        if (labels == null) {
            return false;
        }
        for (String l: labels.trim().split("\\s+")) {
            if (l.equals(label)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return  {@code true} iff running in a Windows environment
     */
    public static boolean windows() {
        return System.getProperty("os.name").toLowerCase(Locale.ENGLISH).contains("windows");
    }
}
