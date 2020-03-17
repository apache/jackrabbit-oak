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

package org.apache.jackrabbit.oak.run.commons;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

public class Utils {

    private Utils() {
    }

    public static String getProductInfo(InputStream pom){
        String version = getProductVersion(pom);

        if (version == null) {
            return "Apache Jackrabbit Oak";
        }

        return "Apache Jackrabbit Oak " + version;
    }

    public static String getProductVersion(InputStream pom) {
        if (pom == null) {
            return null;
        }

        Properties properties = new Properties();

        try {
            properties.load(pom);
        } catch (IOException e) {
            return null;
        }

        return properties.getProperty("version");
    }

    public static void printProductInfo(String[] args, InputStream pom) {
        if(!Arrays.asList(args).contains("--quiet")) {
            System.out.println(Utils.getProductInfo(pom));
        }
    }
}
