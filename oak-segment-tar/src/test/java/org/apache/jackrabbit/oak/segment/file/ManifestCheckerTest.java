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

import static org.apache.jackrabbit.oak.segment.file.ManifestChecker.newManifestChecker;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.file.Files;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ManifestCheckerTest {

    @Rule
    public TemporaryFolder root = new TemporaryFolder(new File("target"));

    private File manifest;

    @Before
    public void setUp() throws Exception {
        manifest = root.newFile();
    }

    @Test(expected = InvalidFileStoreVersionException.class)
    public void testManifestShouldExist() throws Exception {
        Files.delete(manifest.toPath());
        newManifestChecker(manifest, true, 1, 2).checkManifest();
    }

    @Test(expected = IllegalStateException.class)
    public void testInvalidVersion() throws Exception {
        Manifest m = Manifest.load(manifest);
        m.setStoreVersion(0);
        m.save(manifest);
        newManifestChecker(manifest, true, 1, 2).checkManifest();
    }

    @Test(expected = InvalidFileStoreVersionException.class)
    public void testVersionTooLow() throws Exception {
        Manifest m = Manifest.load(manifest);
        m.setStoreVersion(1);
        m.save(manifest);
        newManifestChecker(manifest, true, 2, 3).checkManifest();
    }

    @Test(expected = InvalidFileStoreVersionException.class)
    public void testVersionTooHigh() throws Exception {
        Manifest m = Manifest.load(manifest);
        m.setStoreVersion(4);
        m.save(manifest);
        newManifestChecker(manifest, true, 2, 3).checkManifest();
    }

    @Test
    public void testUpdateExistingManifest() throws Exception {
        Manifest before = Manifest.load(manifest);
        before.setStoreVersion(2);
        before.save(manifest);
        newManifestChecker(manifest, true, 2, 3).checkAndUpdateManifest();
        assertEquals(3, Manifest.load(manifest).getStoreVersion(0));
    }

    @Test
    public void testUpdateNonExistingManifest() throws Exception {
        Files.delete(manifest.toPath());
        newManifestChecker(manifest, false, 2, 3).checkAndUpdateManifest();
        assertEquals(3, Manifest.load(manifest).getStoreVersion(0));
    }

}
