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

package org.apache.jackrabbit.oak.plugins.blob.datastore;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OakFileDataStoreTest {

    @Test
    public void testGetAllIdentifiers() throws Exception {
        File testDir = new File("./target", "oak-fds-test");
        FileUtils.touch(new File(testDir, "a"));
        FileUtils.touch(new File(testDir, "b"));
        FileUtils.touch(new File(testDir, "adir/c"));

        FileDataStore fds = new OakFileDataStore();
        fds.setPath(testDir.getAbsolutePath());
        fds.init(null);

        Iterator<DataIdentifier> dis = fds.getAllIdentifiers();
        Set<String> fileNames = Sets.newHashSet(Iterators.transform(dis, new Function<DataIdentifier, String>() {
            @Override
            public String apply(@Nullable DataIdentifier input) {
                return input.toString();
            }
        }));

        Set<String> expectedNames = Sets.newHashSet("a","b","c");
        assertEquals(expectedNames, fileNames);
        FileUtils.cleanDirectory(testDir);
    }

    @Test
    public void testNoOpMap() throws Exception{
        Map<String, String> noop = new OakFileDataStore.NoOpMap<String, String>();
        noop.put("a","b");
        noop.remove("foo");
        assertTrue(noop.isEmpty());
    }
}
