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
package org.apache.jackrabbit.oak.plugins.blob;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.jackrabbit.oak.plugins.blob.DataStoreCacheUpgradeUtils.DOWNLOAD_DIR;
import static org.apache.jackrabbit.oak.plugins.blob.DataStoreCacheUpgradeUtils.UPLOAD_STAGING_DIR;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link DataStoreCacheUpgradeUtils}
 */
public class DataStoreCacheUpgradeUtilsTest extends AbstractDataStoreCacheTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    File homeDir;
    File path;
    File pendingUploads;

    @Before
    public void setup() throws IOException {
        homeDir = folder.getRoot();
        path = folder.newFolder("repository", "datastore");
        pendingUploads = new File(homeDir + "/" + DataStoreCacheUpgradeUtils.UPLOAD_MAP);
    }

    @Test
    public void upgradeNoDownloads() throws Exception {
        setupUploads("1111110", "2222220", "3333330");

        DataStoreCacheUpgradeUtils.upgrade(homeDir, path, true, true);

        assertFiles(UPLOAD_STAGING_DIR, "1111110", "2222220", "3333330");
        assertFalse(pendingUploads.exists());
    }

    @Test
    public void upgradeNoDownloadsDelPendingFileFalse() throws Exception {
        setupUploads("1111110", "2222220", "3333330");

        DataStoreCacheUpgradeUtils.upgrade(homeDir, path, true, false);

        assertFiles(UPLOAD_STAGING_DIR, "1111110", "2222220", "3333330");
        assertTrue(pendingUploads.exists());
    }

    @Test
    public void upgradeMoveDownloadsFalse() throws Exception {
        setupUploads("1111110", "2222220", "3333330");
        setupDownloads("4444440", "5555550", "6666660");

        DataStoreCacheUpgradeUtils.upgrade(homeDir, path, false, true);

        assertFiles(UPLOAD_STAGING_DIR, "1111110", "2222220", "3333330");
        assertFalse(pendingUploads.exists());
        assertFilesNoMove(DOWNLOAD_DIR, "4444440", "5555550", "6666660");
    }

    @Test
    public void upgradeNoUploads() throws Exception {
        setupDownloads("1111110", "2222220", "3333330");

        DataStoreCacheUpgradeUtils.upgrade(homeDir, path, true, true);

        assertFiles(DOWNLOAD_DIR, "1111110", "2222220", "3333330");
    }

    @Test
    public void upgradeNoUploadMap() throws Exception {
        setupUploads("1111110", "2222220", "3333330");
        FileUtils.deleteQuietly(pendingUploads);

        DataStoreCacheUpgradeUtils.upgrade(homeDir, path, true, true);

        assertFiles(DOWNLOAD_DIR, "1111110", "2222220", "3333330");
        assertFalse(pendingUploads.exists());
    }

    @Test
    public void upgrade() throws Exception {
        upgrade(true);
    }

    @Test
    public void upgradeDelPendingFileFalse() throws Exception {
        upgrade(false);
    }

    private void upgrade(boolean pendingFileDelete) throws Exception {
        setupUploads("1111110", "2222220", "3333330");
        setupDownloads("4444440", "5555550", "6666660");

        DataStoreCacheUpgradeUtils.upgrade(homeDir, path, true, pendingFileDelete);

        assertFiles(UPLOAD_STAGING_DIR, "1111110", "2222220", "3333330");
        if (pendingFileDelete) {
            assertFalse(pendingUploads.exists());
        } else {
            assertTrue(pendingUploads.exists());
        }
        assertFiles(DOWNLOAD_DIR, "4444440", "5555550", "6666660");
    }

    private void setupUploads(String... ids) throws IOException {
        Map<String, Long> pendingMap = Maps.newHashMap();

        for (String id : ids) {
            File f1 = copyToFile(randomStream(Integer.parseInt(id), 4 * 1024), getFile(id, path));
            pendingMap.put(getFileName(id), System.currentTimeMillis());
        }
        serializeMap(pendingMap, pendingUploads);
    }

    private void setupDownloads(String... ids) throws IOException {
        for (String id : ids) {
            copyToFile(randomStream(Integer.parseInt(id), 4 * 1024), getFile(id, path));
        }
    }

    private void assertFiles(String moveFolder, String... ids) throws Exception {
        for (String id : ids) {
            File file = getFile(id, path);
            assertFalse(file.exists());
            file = getFile(id, new File(path, moveFolder));
            assertTrue(file.exists());
            assertTrue(Files.equal(file,
                copyToFile(randomStream(Integer.parseInt(id), 4 * 1024), folder.newFile())));
        }
    }

    private void assertFilesNoMove(String moveFolder, String... ids) throws Exception {
        for (String id : ids) {
            File file = getFile(id, path);
            assertTrue(file.exists());
            assertTrue(Files.equal(file,
                copyToFile(randomStream(Integer.parseInt(id), 4 * 1024), folder.newFile())));
            file = getFile(id, new File(path, moveFolder));
            assertFalse(file.exists());
        }
    }

    private static String getFileName(String name) {
        return name.substring(0, 2) + "/" + name.substring(2, 4) + "/" + name;
    }
}
