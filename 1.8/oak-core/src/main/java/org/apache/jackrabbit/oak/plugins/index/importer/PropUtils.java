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

package org.apache.jackrabbit.oak.plugins.index.importer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

final class PropUtils {

    static Properties loadFromFile(File file) throws IOException {
        try (InputStream is = new BufferedInputStream(new FileInputStream(file))) {
            Properties p = new Properties();
            p.load(is);
            return p;
        }
    }

    static String getProp(Properties p, String key) {
        return checkNotNull(p.getProperty(key), "No property named [%s] found", key);
    }

    static void writeTo(Properties p, File file, String comment) throws IOException {
        try (OutputStream os = new BufferedOutputStream(new FileOutputStream(file))) {
            p.store(os, comment);
        }
    }
}
