/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
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

package org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.jackrabbit.oak.api.blob.BlobUploadOptions;
import org.junit.Test;

public class DataRecordUploadOptionsTest {
    private void verifyUploadOptions(DataRecordUploadOptions options,
                                     boolean expectedIsDomainOverrideIgnored) {
        assertNotNull(options);
        if (expectedIsDomainOverrideIgnored)
            assertTrue(options.isDomainOverrideIgnored());
        else
            assertFalse(options.isDomainOverrideIgnored());
    }

    @Test
    public void testConstruct() {
        BlobUploadOptions blobOptions = new BlobUploadOptions(false);
        DataRecordUploadOptions options = DataRecordUploadOptions.fromBlobUploadOptions(blobOptions);
        verifyUploadOptions(options, false);
    }

    @Test
    public void testDefault() {
        verifyUploadOptions(DataRecordUploadOptions.DEFAULT, false);
    }

    @Test
    public void testConstructFromNullThrowsException() {
        try {
            DataRecordUploadOptions.fromBlobUploadOptions(null);
            fail("Exception expected but not thrown");
        }
        catch (NullPointerException | IllegalArgumentException e) { }
    }

    @Test
    public void testIsDomainOverrideIgnored() {
        verifyUploadOptions(
                DataRecordUploadOptions.fromBlobUploadOptions(
                        new BlobUploadOptions(true)
                ),
                true
        );
    }
}
