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
package org.apache.jackrabbit.oak.plugins.blob;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common utility methods used for DataStore caches.
 */
public class DataStoreCacheUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DataStoreCacheUtils.class);

    /**
     * Delete the file from the given root directory all its empty parent-directories.
     *
     * @param f the file to be deleted
     * @throws IOException
     */
    public static void recursiveDelete(File f, File root) throws IOException {
        if (f.exists()) {
            FileUtils.forceDelete(f);
            LOG.info("Deleted file [{}]", f);
        }

        // delete empty parent folders (except the main directory)
        while (true) {
            f = f.getParentFile();
            if ((f == null) || f.equals(root)
                || (f.list() == null)
                || (f.list().length > 0)) {
                break;
            }
            LOG.debug("Deleted directory [{}], [{}]", f, f.delete());
        }
    }

    /**
     * Obtain a placeholder in the file system folder for the given identifier.
     *
     * @param id of the file
     * @return file handle
     */
    public static File getFile(String id, File root) {
        File file = root;
        file = new File(file, id.substring(0, 2));
        file = new File(file, id.substring(2, 4));
        file = new File(file, id.substring(4, 6));
        file.mkdirs();
        return new File(file, id);
    }
}
