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
package org.apache.jackrabbit.oak.plugins.document.blob;

import static org.junit.Assume.assumeTrue;

import java.util.Random;

import org.apache.jackrabbit.oak.plugins.document.rdb.RDBBlobStoreFriend;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RDBBlobStorePerformanceTest extends RDBBlobStoreTest {

    private static final Logger LOG = LoggerFactory.getLogger(RDBBlobStorePerformanceTest.class);
    private static final boolean ENABLED = Boolean.getBoolean(RDBBlobStorePerformanceTest.class.getSimpleName());

    public RDBBlobStorePerformanceTest(RDBBlobStoreFixture bsf) {
        super(bsf);
        assumeTrue(ENABLED);
    }

    @Test
    public void testInsertSmallBlobs() throws Exception {
        int size = 1500;
        long duration = 5000;
        long end = System.currentTimeMillis() + duration;
        int cnt = 0;
        int errors = 0;
        Random r = new Random(0);

        while (System.currentTimeMillis() < end) {
            byte[] data = new byte[size];
            r.nextBytes(data);
            byte[] digest = super.getDigest(data);
            try {
                RDBBlobStoreFriend.storeBlock(super.blobStore, digest, 0, data);
                cnt += 1;
            } catch (Exception ex) {
                LOG.debug("insert failed", ex);
                errors += 1;
            }
        }

        LOG.info("inserted " + cnt + " blocks of size " + size + " into " + super.blobStoreName + " (" + errors + " errors) in "
                + duration + "ms (" + (cnt * 1000) / duration + " blocks/s)");
    }
}
