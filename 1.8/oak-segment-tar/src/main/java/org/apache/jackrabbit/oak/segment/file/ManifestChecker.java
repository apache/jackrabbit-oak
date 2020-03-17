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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.IOException;

public class ManifestChecker {

    public static ManifestChecker newManifestChecker(File path, boolean shouldExist, int minStoreVersion, int maxStoreVersion) {
        checkArgument(path != null, "path");
        checkArgument(minStoreVersion > 0, "minStoreVersion");
        checkArgument(maxStoreVersion > 0, "maxStoreVersion");
        return new ManifestChecker(path, shouldExist, minStoreVersion, maxStoreVersion);
    }

    private final File file;

    private final boolean shouldExist;

    private final int minStoreVersion;

    private final int maxStoreVersion;

    private ManifestChecker(File file, boolean shouldExist, int minStoreVersion, int maxStoreVersion) {
        this.file = file;
        this.shouldExist = shouldExist;
        this.minStoreVersion = minStoreVersion;
        this.maxStoreVersion = maxStoreVersion;
    }

    void checkAndUpdateManifest() throws IOException, InvalidFileStoreVersionException {
        Manifest manifest = openManifest();
        checkManifest(manifest);
        updateManifest(manifest);
    }

    public void checkManifest() throws IOException, InvalidFileStoreVersionException {
        checkManifest(openManifest());
    }

    private Manifest openManifest() throws IOException, InvalidFileStoreVersionException {
        if (file.exists()) {
            return Manifest.load(file);
        }
        if (shouldExist) {
            throw new InvalidFileStoreVersionException("Using oak-segment-tar, but oak-segment should be used");
        }
        return Manifest.empty();
    }

    private void checkManifest(Manifest manifest) throws InvalidFileStoreVersionException {
        checkStoreVersion(manifest.getStoreVersion(maxStoreVersion));
    }

    private void checkStoreVersion(int storeVersion) throws InvalidFileStoreVersionException {
        // A store version less than or equal to the highest invalid value means
        // that something or someone is messing up with the manifest. This error
        // is not recoverable and is thus represented as an ISE.
        if (storeVersion <= 0) {
            throw new IllegalStateException("Invalid store version");
        }
        if (storeVersion < minStoreVersion) {
            throw new InvalidFileStoreVersionException("Using a too recent version of oak-segment-tar");
        }
        if (storeVersion > maxStoreVersion) {
            throw new InvalidFileStoreVersionException("Using a too old version of oak-segment tar");
        }
    }

    private void updateManifest(Manifest manifest) throws IOException {
        // Always update the store version to the maximum supported store
        // version. In doing so, we prevent older implementations from tampering
        // with the store's data, which from this moment on could be written in
        // a format that an older implementation might not be able to
        // understand.
        manifest.setStoreVersion(maxStoreVersion);
        manifest.save(file);
    }

}
