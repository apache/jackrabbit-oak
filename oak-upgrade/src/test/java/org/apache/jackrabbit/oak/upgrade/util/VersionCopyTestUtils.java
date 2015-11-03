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
package org.apache.jackrabbit.oak.upgrade.util;

import java.util.ArrayList;
import java.util.List;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.version.Version;
import javax.jcr.version.VersionHistory;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.upgrade.version.VersionCopyConfiguration;

public class VersionCopyTestUtils {

    public static String createVersionableNode(Session session, String versionablePath)
            throws RepositoryException, InterruptedException {
        final VersionManager versionManager = session.getWorkspace().getVersionManager();
        final Node versionable = JcrUtils.getOrCreateUniqueByPath(session.getRootNode(), versionablePath,
                JcrConstants.NT_UNSTRUCTURED);
        versionable.addMixin("mix:versionable");
        versionable.setProperty("version", "root");
        session.save();

        final String path = versionable.getPath();
        final List<String> versionNames = new ArrayList<String>();
        for (int i = 0; i < 3; i++) {
            versionable.setProperty("version", "1." + i);
            session.save();
            final Version v = versionManager.checkpoint(path);
            versionNames.add(v.getName());
        }

        final VersionHistory history = versionManager.getVersionHistory(path);
        for (final String versionName : versionNames) {
            history.addVersionLabel(versionName, String.format("version %s", versionName), false);
        }
        return history.getPath();
    }

    public static boolean isVersionable(Session session, String path) throws RepositoryException {
        return session.getNode(path).isNodeType(JcrConstants.MIX_VERSIONABLE);
    }

    public interface VersionCopySetup {
        void setup(VersionCopyConfiguration config);
    }
}
