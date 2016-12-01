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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.blob.DataStoreCacheUtils.recursiveDelete;

/**
 * Utility methods to upgrade Old DataStore cache
 * {@link org.apache.jackrabbit.core.data.CachingDataStore}.
 */
public class DataStoreCacheUpgradeUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DataStoreCacheUpgradeUtils.class);

    static final String UPLOAD_MAP = "async-pending-uploads.ser";
    static final String UPLOAD_STAGING_DIR = UploadStagingCache.UPLOAD_STAGING_DIR;
    static final String DOWNLOAD_DIR = FileCache.DOWNLOAD_DIR;

    private static Map<String, Long> deSerializeUploadMap(File homeDir) {
        Map<String, Long> asyncUploadMap = Maps.newHashMap();

        File asyncUploadMapFile = new File(homeDir, UPLOAD_MAP);
        if (asyncUploadMapFile.exists()) {
            String path = asyncUploadMapFile.getAbsolutePath();

            InputStream fis = null;
            try {
                fis = new BufferedInputStream(new FileInputStream(path));
                ObjectInput input = new ObjectInputStream(fis);
                asyncUploadMap = (Map<String, Long>) input.readObject();
            } catch (Exception e) {
                LOG.warn("Error in reading pending uploads map [{}] from location [{}]", UPLOAD_MAP,
                    homeDir);
            } finally {
                IOUtils.closeQuietly(fis);
            }
            LOG.debug("AsyncUploadMap read [{}]", asyncUploadMap);
        }
        return asyncUploadMap;
    }

    private static void deleteSerializedUploadMap(File homeDir) {
        File uploadMap = new File(homeDir, UPLOAD_MAP);
        FileUtils.deleteQuietly(uploadMap);
        LOG.info("Deleted asyncUploadMap [{}] from [{}]", UPLOAD_MAP, homeDir);
    }

    private static boolean notInExceptions(File file, List<String> exceptions) {
        String parent = file.getParent();
        for (String exception : exceptions) {
            if (parent.contains(exception)) {
                return true;
            }
        }
        return false;
    }

    public static void moveDownloadCache(final File path) {
        final List<String> exceptions = ImmutableList.of("tmp", UPLOAD_STAGING_DIR, DOWNLOAD_DIR);
        File newDownloadDir = new File(path, DOWNLOAD_DIR);

        Iterator<File> iterator =
            Files.fileTreeTraverser().postOrderTraversal(path)
                .filter(new Predicate<File>() {
                    @Override public boolean apply(File input) {
                        return input.isFile()
                            && !input.getParentFile().equals(path)
                            && !notInExceptions(input, exceptions);
                    }
                }).iterator();

        while (iterator.hasNext()) {
            File download = iterator.next();
            LOG.trace("Download cache file absolute pre-upgrade path " + download);

            File newDownload = new File(newDownloadDir,
                download.getAbsolutePath().substring(path.getAbsolutePath().length()));
            newDownload.getParentFile().mkdirs();
            LOG.trace("Downloaded cache file absolute post-upgrade path " + newDownload);

            try {
                FileUtils.moveFile(download, newDownload);
                LOG.info("Download cache file [{}] moved to [{}]", download, newDownload);
                recursiveDelete(download, path);
            } catch (Exception e) {
                LOG.warn("Unable to move download cache file [{}] to [{}]", download, newDownload);
            }
        }
    }

    public static void movePendingUploadsToStaging(File homeDir, File path, boolean deleteMap) {
        File newUploadDir = new File(path, UPLOAD_STAGING_DIR);

        Map<String, Long> pendingUploads = deSerializeUploadMap(homeDir);
        Iterator<String> pendingFileIter = pendingUploads.keySet().iterator();

        while(pendingFileIter.hasNext()) {
            String file = pendingFileIter.next();
            File upload = new File(path, file);
            LOG.trace("Pending upload absolute path " + upload.getAbsolutePath());

            File newUpload = new File(newUploadDir, file);
            LOG.trace("Pending upload upgrade absolute path " + newUpload.getAbsolutePath());

            newUpload.getParentFile().mkdirs();

            if (upload.exists()) {
                LOG.trace(upload + " File exists");
                try {
                    FileUtils.moveFile(upload, newUpload);
                    LOG.info("Pending upload file [{}] moved to [{}]", upload, newUpload);
                    recursiveDelete(upload, path);
                } catch (Exception e) {
                    LOG.warn("Unable to move pending upload file [{}] to [{}]", upload, newUpload);
                }
            } else {
                LOG.warn("File [{}] does not exist", upload);
            }
        }

        if (deleteMap) {
            deleteSerializedUploadMap(homeDir);
        }
    }

    public static void upgrade(File homeDir, File path, boolean moveCache, boolean deleteMap) {
        movePendingUploadsToStaging(homeDir, path, deleteMap);

        if (moveCache) {
            moveDownloadCache(path);
        }
    }
}
