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
package org.apache.jackrabbit.oak.jcr.binary.fixtures.nodestore;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.binary.fixtures.datastore.DataStoreFixture;
import org.jetbrains.annotations.Nullable;

/**
 * Utility methods for NodeStoreFixtures and DataStoreFixtures.
 */
public abstract class FixtureUtils {

    /** Return a nice label for jUnit Parameterized tests for fixtures */
    public static String getFixtureLabel(NodeStoreFixture fixture,
                                  DataStoreFixture dataStoreFixture) {
        
        String nodeStoreName = fixture.getClass().getSimpleName();
        String name = StringUtils.removeEnd(nodeStoreName, "Fixture");
        if (dataStoreFixture != null) {
            String dataStoreName = dataStoreFixture.getClass().getSimpleName();
            return name + "_" + StringUtils.removeEnd(dataStoreName, "Fixture");
        }
        return name;

    }

    /** Create a temporary folder inside the maven build folder "target". */
    public static File createTempFolder() throws IOException {
        // create temp folder inside maven's "target" folder
        File parent = new File("target");
        File tempFolder = File.createTempFile("junit", "", parent);
        tempFolder.delete();
        tempFolder.mkdir();
        return tempFolder;
    }

    /**
     * Load data store *.properties from path in system property, local file or file inside user home directory.
     * Returns null if no file was found.
     */
    @Nullable
    public static Properties loadDataStoreProperties(String systemProperty,
                                                     String defaultFileName,
                                                     String homeFolderName) {
        Properties props = new Properties();
        try {
            File file = new File(System.getProperty(systemProperty, defaultFileName));
            if (!file.exists()) {
                file = Paths.get(System.getProperty("user.home"), homeFolderName, defaultFileName).toFile();
            }
            props.load(new FileReader(file));
        } catch (IOException e) {
            return null;
        }
        return props;
    }
}
