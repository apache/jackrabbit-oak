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

package org.apache.jackrabbit.oak.segment.file;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

class Manifest {

    private static final String STORE_VERSION = "store.version";

    /**
     * Loads the manifest from a file.
     *
     * @param file The file to load the manifest from.
     * @return A manifest file.
     * @throws IOException If any error occurs when loading the manifest.
     */
    static Manifest load(File file) throws IOException {
        Properties properties = new Properties();
        try (FileReader r = new FileReader(file)) {
            properties.load(r);
        }
        return new Manifest(properties);
    }

    /**
     * Creates an empty manifest file.
     *
     * @return A manifest file.
     */
    static Manifest empty() {
        return new Manifest(new Properties());
    }

    private final Properties properties;

    private Manifest(Properties properties) {
        this.properties = properties;
    }

    /**
     * Return the store version saved in this manifest or a user provided value
     * if no valid store version is saved in the manifest.
     *
     * @param otherwise The value that will be returned if no valid store
     *                  version is saved in this manifest.
     * @return The store version stored in this manifest or the user provided
     * default value.
     */
    int getStoreVersion(int otherwise) {
        return getIntegerProperty(STORE_VERSION, otherwise);
    }

    /**
     * Set the store version in this manifest.
     *
     * @param version The store version.
     */
    void setStoreVersion(int version) {
        setIntegerProperty(STORE_VERSION, version);
    }

    /**
     * Save the manifest to the specified file.
     *
     * @param file The file to save the manifest to.
     * @throws IOException if an error occurs while saving the manifest.
     */
    void save(File file) throws IOException {
        properties.store(new FileWriter(file), null);
    }

    private int getIntegerProperty(String name, int otherwise) {
        Object value = properties.get(name);

        if (value == null) {
            return otherwise;
        }

        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            return otherwise;
        }
    }

    private void setIntegerProperty(String name, int value) {
        properties.put(name, Integer.toString(value));
    }

}
