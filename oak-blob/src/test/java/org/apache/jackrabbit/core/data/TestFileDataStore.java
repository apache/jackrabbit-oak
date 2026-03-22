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

package org.apache.jackrabbit.core.data;

import java.io.File;
import java.util.Properties;

import javax.jcr.RepositoryException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test cases to test {@link FileDataStore}
 */
public class TestFileDataStore extends TestCaseBase {

    protected static final Logger LOG = LoggerFactory.getLogger(TestFileDataStore.class);

    String fsPath;

    @Override
    protected DataStore createDataStore() throws RepositoryException {
        FileDataStore fds = new FileDataStore();
        Properties props = loadProperties("/fs.properties");
        String pathValue = props.getProperty(FSBackend.FS_BACKEND_PATH);
        if (pathValue != null && !"".equals(pathValue.trim())) {
            fsPath = pathValue + "/fds" + "-"
                + String.valueOf(randomGen.nextInt(100000)) + "-"
                + String.valueOf(randomGen.nextInt(100000));
        } else {
            fsPath = dataStoreDir + "/repository/datastore";
        }
        LOG.info("path [{}] set.", fsPath);
        fds.setPath(fsPath);
        fds.init(dataStoreDir);
        return fds;
    }

    @Override
    protected void tearDown() {
        LOG.info("cleaning fsPath [{}]", fsPath);
        File f = new File(fsPath);
        try {
            for (int i = 0; i < 4 && f.exists(); i++) {
                FileUtils.deleteQuietly(f);
                Thread.sleep(2000);
            }
        } catch (Exception ignore) {

        }
        super.tearDown();
    }
}
