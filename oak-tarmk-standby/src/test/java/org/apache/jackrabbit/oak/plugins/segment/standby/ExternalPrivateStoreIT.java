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
package org.apache.jackrabbit.oak.plugins.segment.standby;

import static org.apache.jackrabbit.oak.plugins.segment.SegmentTestUtils.createTmpTargetDir;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.junit.After;


public class ExternalPrivateStoreIT extends DataStoreTestBase {

    private File primaryStore;
    private File secondaryStore;

    @After
    public void after() {
        closeServerAndClient();
        try {
            FileUtils.deleteDirectory(primaryStore);
            FileUtils.deleteDirectory(secondaryStore);
        } catch (IOException e) {
        }
    }

    @Override
    protected FileStore setupPrimary(File d) throws Exception {
        primaryStore = createTmpTargetDir("ExternalStoreITPrimary");
        return setupFileDataStore(d, primaryStore.getAbsolutePath());
    }

    @Override
    protected FileStore setupSecondary(File d) throws Exception {
        secondaryStore = createTmpTargetDir("ExternalStoreITSecondary");
        return setupFileDataStore(d, secondaryStore.getAbsolutePath());
    }

}
