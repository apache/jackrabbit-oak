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
package org.apache.jackrabbit.oak.plugins.version;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.migration.version.VersionHistoryUtil;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import static org.apache.jackrabbit.oak.commons.PathUtils.relativize;
import static org.apache.jackrabbit.oak.spi.version.VersionConstants.VERSION_STORE_PATH;

public class ReadWriteVersionManagerUtil {

    /**
     * Remove a version at the given path.
     *
     * @param builder the root builder.
     * @param path the path of a version to remove.
     * @throws CommitFailedException if a constraint is violated.
     */
    public static void removeVersion(NodeBuilder builder, String path)
            throws CommitFailedException {
        ReadWriteVersionManager vMgr = new ReadWriteVersionManager(
                VersionHistoryUtil.getVersionStorage(builder),
                builder
        );
        String relPath = relativize(VERSION_STORE_PATH, path);
        vMgr.removeVersion(relPath);
    }
}
