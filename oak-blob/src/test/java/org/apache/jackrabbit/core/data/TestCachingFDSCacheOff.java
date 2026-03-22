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

import java.util.Properties;

import javax.jcr.RepositoryException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCachingFDSCacheOff extends TestFileDataStore {

    protected static final Logger LOG = LoggerFactory.getLogger(TestCachingFDS.class);
    
    protected DataStore createDataStore() throws RepositoryException {
        CachingFDS cacheFDS = new CachingFDS();
        Properties props = loadProperties("/fs.properties");
        String pathValue = props.getProperty(FSBackend.FS_BACKEND_PATH);
        if (pathValue != null && !"".equals(pathValue.trim())) {
            fsPath = pathValue + "/cachingFds" + "-"
                + String.valueOf(randomGen.nextInt(100000)) + "-"
                + String.valueOf(randomGen.nextInt(100000));
        } else {
            fsPath = dataStoreDir + "/cachingFDS";
        }
        props.setProperty(FSBackend.FS_BACKEND_PATH, fsPath);
        cacheFDS.setProperties(props);
        cacheFDS.setSecret("12345");
        // disable asynchronous writing in testing.
        cacheFDS.setAsyncUploadLimit(0);
        cacheFDS.setCacheSize(0);
        cacheFDS.init(dataStoreDir);
        return cacheFDS;
    }
}
