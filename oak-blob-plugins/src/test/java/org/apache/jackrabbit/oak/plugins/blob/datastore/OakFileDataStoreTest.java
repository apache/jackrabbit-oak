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
import org.apache.commons.io.FilenameUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OakFileDataStoreTest {

    @Test
    public void testGetAllIdentifiersRelative1() throws Exception {
        File f = new File("./target/oak-fds-test1");
        testGetAllIdentifiers(f.getAbsolutePath(), f.getPath());
    }

    @Test
    public void testGetAllIdentifiersRelative2() throws Exception {
        File f = new File("./target", "/fds/../oak-fds-test2");
        testGetAllIdentifiers(FilenameUtils.normalize(f.getAbsolutePath()), f.getPath());
    }

    @Test
    public void testGetAllIdentifiers() throws Exception {
        File f = new File("./target", "oak-fds-test3");
        testGetAllIdentifiers(f.getAbsolutePath(), f.getPath());
    }

    private void testGetAllIdentifiers(String path, String unnormalizedPath) throws Exception {
        File testDir = new File(path);
        FileUtils.touch(new File(testDir, "ab/cd/ef/abcdef"));
        FileUtils.touch(new File(testDir, "bc/de/fg/bcdefg"));
        FileUtils.touch(new File(testDir, "cd/ef/gh/cdefgh"));
        FileUtils.touch(new File(testDir, "c"));

        FileDataStore fds = new OakFileDataStore();
        fds.setPath(unnormalizedPath);
        fds.init(null);

        Iterator<DataIdentifier> dis = fds.getAllIdentifiers();
        Set<String> fileNames = Sets.newHashSet(Iterators.transform(dis, new Function<DataIdentifier, String>() {
            @Override
            public String apply(@Nullable DataIdentifier input) {
                return input.toString();
            }
        }));

        Set<String> expectedNames = Sets.newHashSet("abcdef","bcdefg","cdefgh");
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
