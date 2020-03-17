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
package org.apache.jackrabbit.oak.upgrade.cli;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Util {

    private static final Logger log = LoggerFactory.getLogger(Util.class);

    private Util() {
    }

    public static void unzip(InputStream is, File targetDir) throws IOException {
        long start = System.currentTimeMillis();
        log.info("Unzipping to {}", targetDir.getAbsolutePath());
        final ZipInputStream zis = new ZipInputStream(is);
        try {
            ZipEntry entry = null;
            while((entry = zis.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    new File(targetDir, entry.getName()).mkdirs();
                } else {
                    File target = new File(targetDir, entry.getName());
                    target.getParentFile().mkdirs();
                    OutputStream output = new FileOutputStream(target);
                    try {
                        IOUtils.copy(zis, output);
                    } finally {
                        output.close();
                    }
                }
            }
        } finally {
            zis.close();
        }
        final long delta = System.currentTimeMillis() - start;
        if(delta > 1000L) {
            log.info("Unzip took {} msec", delta);
        }
    }
}
