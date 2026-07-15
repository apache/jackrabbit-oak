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
package org.apache.jackrabbit.oak.fixture;

import static org.apache.jackrabbit.core.config.RepositoryConfigurationParser.REPOSITORY_HOME_VARIABLE;

import java.io.File;
import java.io.InputStream;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.jcr.Repository;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.RepositoryImpl;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.xml.sax.InputSource;

public class JackrabbitRepositoryFixture implements RepositoryFixture {

    private final File base;

    private final int bundleCacheSize;

    private RepositoryImpl[] cluster;

    public JackrabbitRepositoryFixture(File base, int bundleCacheSize) {
        this.base = base;
        this.bundleCacheSize = bundleCacheSize;
    }

    @Override
    public boolean isAvailable(int n) {
        return n == 1;
    }

    @Override
    public Repository[] setUpCluster(int n) throws Exception {
        if (n == 1) {
            String name = "Jackrabbit-" + System.currentTimeMillis();
            File directory = new File(base, name);

            Properties variables = new Properties(System.getProperties());
            variables.setProperty(
                    REPOSITORY_HOME_VARIABLE, directory.getPath());
            variables.setProperty(
                    "bundleCacheSize", Integer.toString(bundleCacheSize));
            InputStream xml = getClass().getResourceAsStream("repository.xml");
            RepositoryConfig config = RepositoryConfig.create(
                    new InputSource(xml), variables);

            // Prevent Derby from polluting the current directory
            System.setProperty(
                    "derby.stream.error.file",
                    new File(directory, "derby.log").getPath());

            RepositoryImpl repository = RepositoryImpl.create(config);
            this.cluster = new RepositoryImpl[] { repository };
            return new Repository[] { repository };
        } else {
            throw new UnsupportedOperationException("TODO");
        }
    }

    @Override
    public void syncRepositoryCluster(Repository... nodes) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public void tearDownCluster() {
        for (RepositoryImpl repository : cluster) {
            File directory = new File(repository.getConfig().getHomeDir());
            repository.shutdown();
            try {
                DriverManager.getConnection("jdbc:derby:;shutdown=true");
            } catch (SQLException e) {
                // ignore
            }
            FileUtils.deleteQuietly(directory);
        }
    }

    @Override
    public String toString() {
        return "Jackrabbit";
    }

}
