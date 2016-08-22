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

package org.apache.jackrabbit.oak.segment.file;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ManifestTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Test
    public void defaultStoreVersionShouldBeReturned() throws Exception {
        assertEquals(42, Manifest.load(folder.newFile()).getStoreVersion(42));
    }

    @Test
    public void storeVersionShouldBeReturned() throws Exception {
        File file = folder.newFile();

        Manifest write = Manifest.empty();
        write.setStoreVersion(42);
        write.save(file);

        Manifest read = Manifest.load(file);
        assertEquals(42, read.getStoreVersion(0));
    }

}
