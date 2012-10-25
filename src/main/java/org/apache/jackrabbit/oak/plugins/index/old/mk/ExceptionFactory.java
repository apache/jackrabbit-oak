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
package org.apache.jackrabbit.oak.plugins.index.old.mk;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.jackrabbit.mk.api.MicroKernelException;

/**
 * An exception factory.
 */
public class ExceptionFactory {

    private static final String POM = "META-INF/maven/org.apache.jackrabbit/oak-core/pom.properties";

    private static String version;

    public static MicroKernelException convert(Exception e) {
        if (e instanceof MicroKernelException) {
            return (MicroKernelException) e;
        }
        return new MicroKernelException(e.getMessage() + " " + getVersion(), e);
    }

    public static MicroKernelException get(String s) {
        return new MicroKernelException(s + " " + getVersion());
    }

    public static String getVersion() {
        if (version == null) {
            try {
                InputStream in = ExceptionFactory.class.getClassLoader().getResourceAsStream(POM);
                if (in == null) {
                    in = new FileInputStream("target/maven-archiver/pom.properties");
                }
                Properties prop = new Properties();
                prop.load(in);
                in.close();
                version = "[" + prop.getProperty("artifactId") + "-" + prop.getProperty("version") + "]";
            } catch (IOException e) {
                version = "";
            }
        }
        return version;
    }

}
