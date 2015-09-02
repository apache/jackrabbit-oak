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

import static com.google.common.base.Objects.equal;
import static java.lang.Boolean.parseBoolean;
import static java.lang.System.getenv;

/**
 * Utility class for ITs to determine the environment running in.
 */
public final class CIHelper {
    private CIHelper() { }

    /**
     * @return  {@code true} iff running on
     * http://ci.apache.org/builders/oak-trunk-win7
     */
    public static boolean buildBotTrunkWin7() {
        String user = getenv("USERDOMAIN");
        return user != null && user.startsWith("bb-win7");
    }

    /**
     * @return  {@code true} iff running on
     * http://ci.apache.org/builders/oak-trunk
     */
    public static boolean buildBotTrunkLinux() {
        String user = getenv("USER");
        return user != null && user.startsWith("buildslave3");
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
        return equal(getenv("PROFILE"), "pedantic");
    }

    /**
     * @return  {@code true} iff running on with {@code PROFILE=unittesting}
     * @deprecated Travis builds do not use PROFILE anymore. Use {@link #travis()} instead.
     */
    public static boolean travisUnitTesting() {
        return equal(getenv("PROFILE"), "unittesting");
    }

    /**
     * @return  {@code true} iff running on with {@code PROFILE=integrationTesting}
     * @deprecated Travis builds do not use PROFILE anymore. Use {@link #travis()} instead.
     */
    public static boolean travisIntegrationTesting() {
        return equal(getenv("PROFILE"), "integrationTesting");
    }

}
