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
package org.apache.jackrabbit.oak.run;

import static java.util.Arrays.copyOfRange;
import static org.apache.jackrabbit.oak.commons.IOUtils.closeQuietly;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

public final class Main {

    private Main() {
        // Prevent instantiation.
    }

    public static void main(String[] args) throws Exception {
        printProductInfo(args);

        Mode mode = Mode.SERVER;

        if (args.length > 0) {
            mode = getMode(args[0]);

            if (mode == null) {
                mode = Mode.HELP;
            }

            args = copyOfRange(args, 1, args.length);
        }

        mode.execute(args);
    }

    public static String getProductInfo(){
        String version = getProductVersion();

        if (version == null) {
            return "Apache Jackrabbit Oak";
        }

        return "Apache Jackrabbit Oak " + version;
    }

    private static String getProductVersion() {
        InputStream stream = Main.class.getResourceAsStream("/META-INF/maven/org.apache.jackrabbit/oak-run/pom.properties");

        try {
            return getProductVersion(stream);
        } finally {
            closeQuietly(stream);
        }
    }

    private static String getProductVersion(InputStream stream) {
        Properties properties = new Properties();

        try {
            properties.load(stream);
        } catch (IOException e) {
            return null;
        }

        return properties.getProperty("version");
    }

    private static void printProductInfo(String[] args) {
        if(!Arrays.asList(args).contains("--quiet")) {
            System.out.println(getProductInfo());
        }
    }

    private static Mode getMode(String name) {
        try {
            return Mode.valueOf(name.toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

}
