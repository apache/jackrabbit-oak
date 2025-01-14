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

import org.apache.jackrabbit.guava.common.base.Strings;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.RemoteDockerImage;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RdbConnectionUtils {

    private static final Logger LOG = LoggerFactory.getLogger(RdbConnectionUtils.class);

    public static final String URL = System.getProperty("rdb.jdbc-url", "jdbc:h2:file:./{fname}oaktest;DB_CLOSE_ON_EXIT=FALSE");
    public static final String USERNAME = System.getProperty("rdb.jdbc-user", "sa");
    public static final String PASSWD = System.getProperty("rdb.jdbc-passwd", "");
    public static final String IMG = System.getProperty("rdb.docker-image", "");

    private static final boolean RDB_AVAILABLE;
    private static GenericContainer<?> rdbContainer;

    private static int exposedPort = getPortFromJdbcURL(URL);

    static {
        boolean dockerAvailable = false;
        boolean imageAvailable = false;
        try {
            dockerAvailable = checkDockerAvailability();
            if (dockerAvailable) {
                imageAvailable = checkImageAvailability();
            } else {
                LOG.info("docker not available");
            }
        } catch (Throwable t) {
            LOG.error("not able to pull specified docker image: {}, error: ", IMG, t);
        }
        RDB_AVAILABLE = dockerAvailable && imageAvailable;
        if (RDB_AVAILABLE) {
            rdbContainer = new GenericContainer<>(DockerImageName.parse(IMG))
                    .withPrivilegedMode(true)
                    .withExposedPorts(exposedPort)
                    .withStartupTimeout(Duration.ofMinutes(15));
            try {
                long startTime = Instant.now().toEpochMilli();
                rdbContainer.start();
                LOG.info("RDB container started in: " + (Instant.now().toEpochMilli() - startTime) + " ms");
                String url = RdbConnectionUtils.mapJdbcURL();
                LOG.info("Mapped JDBC URL is {}.", url);
                boolean containerReady = false;
                LOG.info("Trying to connect to {}", url);
                for (int k = 0; k < 30 && !containerReady; k++) {
                    Thread.sleep(10000);
                    Connection connection = null;
                    try {
                        DataSource dataSource = RDBDataSourceFactory.forJdbcUrl(url, RdbConnectionUtils.USERNAME, RdbConnectionUtils.PASSWD);
                        connection = dataSource.getConnection();
                        containerReady = true;
                    } catch (SQLException expected) {
                        LOG.info("Failed to connect to {}, will retry", url);
                    } finally {
                        if (connection != null) {
                            try {
                                connection.close();
                            } catch (SQLException expected) {}
                        }
                    }
                    if (containerReady) {
                        LOG.info("Container ready");
                    } else {
                        LOG.error("Failed to connect to {} within timeout", url);
                    }
                }
            } catch (Exception e) {
                LOG.error("error while starting RDB container, error: ", e);
            }
        }
    }

    public static int getPortFromJdbcURL(String jdbcURL) {
        String normalizedJdbcUri = jdbcURL.replaceFirst("@//", "//").replaceFirst("@", "//");
        Pattern pattern = Pattern.compile("//[^:/]+(:(\\d+))?");
        Matcher matcher = pattern.matcher(normalizedJdbcUri);
        if (matcher.find()) {
            if (matcher.groupCount() > 1) {
                try {
                    return Integer.parseInt(matcher.group(2));
                } catch (NumberFormatException ignored) {
                    //should not happen
                }
            }
        }
        return -1;
    }

    public static String mapJdbcURL() {
        String jdbcUrl = URL;
        if (RDB_AVAILABLE) {
            String normalizedJdbcUri = URL.replaceFirst("@//", "//").replaceFirst("@", "//");
            Pattern pattern = Pattern.compile("//[^:/]+(:(\\d+))?");
            Matcher matcher = pattern.matcher(normalizedJdbcUri);
            if (matcher.find()) {
                if (matcher.groupCount() > 1) {
                    jdbcUrl = matcher.replaceFirst("//" + rdbContainer.getHost() + ":" + rdbContainer.getMappedPort(exposedPort));
                }
            }
        }
        return jdbcUrl.replace("{fname}", (new File("target")).isDirectory() ? "target/" : "");
    }

    private static boolean checkImageAvailability() throws TimeoutException {
        if (Strings.isNullOrEmpty(IMG)) {
            return false;
        }
        RemoteDockerImage remoteDockerImage = new RemoteDockerImage(DockerImageName.parse(IMG));
        remoteDockerImage.get(60, TimeUnit.MINUTES);
        return true;
    }

    private static boolean checkDockerAvailability() {
        return DockerClientFactory.instance().isDockerAvailable();
    }
}
