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
package org.apache.jackrabbit.oak.plugins.document;

import org.apache.jackrabbit.oak.plugins.document.rdb.RdbDockerRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RdbUtils {

    private static final Logger LOG = LoggerFactory.getLogger(RdbUtils.class);

    public static final String URL = System.getProperty("rdb.jdbc-url", "jdbc:h2:file:./{fname}oaktest;DB_CLOSE_ON_EXIT=FALSE");
    public static final String USERNAME = System.getProperty("rdb.jdbc-user", "sa");
    public static final String PASSWD = System.getProperty("rdb.jdbc-passwd", "");
    public static final String IMAGE = System.getProperty("rdb.docker-image", "");

    private static AtomicInteger port = new AtomicInteger(-1);
    private static AtomicReference<String> host = new AtomicReference<>("localhost");

    static {
        try {
            if (RdbDockerRule.isDockerImageAvailable()) {
                RdbDockerRule rule = new RdbDockerRule();
                rule.apply(new Statement() {
                    @Override
                    public void evaluate() {
                        port.set(rule.getMappedPort());
                    }
                }, Description.EMPTY).evaluate();
            }
        } catch (Throwable t) {
            LOG.debug("Failed to initialize docker container", t);
        }
    }

    public static String mapJdbcURL() {
        return mapJdbcURL(URL);
    }

    public static String mapJdbcURL(String jdbcURL) {
        if (port.get() > -1) {
            String normalizedJdbcUri = jdbcURL.replaceFirst("@//", "//").replaceFirst("@", "//");
            Pattern pattern = Pattern.compile("//[^:/]+(:(\\d+))?");
            Matcher matcher = pattern.matcher(normalizedJdbcUri);
            if (matcher.find()) {
                if (matcher.groupCount() > 1) {
                    return matcher.replaceFirst("//" + host + ":" + port);
                }
            }
        }
        return jdbcURL;
    }
}
