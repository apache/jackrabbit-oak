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

import javax.jcr.RepositoryException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test {@link CachingDataStore} with InMemoryBackend and local cache off.
 */

public class TestInMemDsCacheOff extends TestCaseBase {

    protected static final Logger LOG = LoggerFactory.getLogger(TestInMemDsCacheOff.class);
    @Override
    protected DataStore createDataStore() throws RepositoryException {
        InMemoryDataStore inMemDS = new InMemoryDataStore();
        inMemDS.setProperties(null);
        inMemDS.init(dataStoreDir);
        inMemDS.setSecret("12345");
        inMemDS.setCacheSize(0);
        return inMemDS;
    }
}
