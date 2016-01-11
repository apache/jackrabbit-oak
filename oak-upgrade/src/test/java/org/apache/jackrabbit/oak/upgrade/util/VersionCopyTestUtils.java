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
import javax.jcr.version.VersionIterator;
import javax.jcr.version.VersionManager;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.upgrade.version.VersionCopyConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class VersionCopyTestUtils {

    public static List<String> LABELS = ImmutableList.of("1.0", "1.1", "1.2");

    public static Node getOrAddNode(Node parent, String relPath) throws RepositoryException {
        Node currentParent = parent;
        for (final String name : PathUtils.elements(relPath)) {
            if (!currentParent.hasNode(name)) {
                currentParent.addNode(name, JcrConstants.NT_UNSTRUCTURED);
            }
            currentParent = currentParent.getNode(name);
        }
        return currentParent;
    }

    public static Node getOrAddNodeWithMixins(Node parent, String name, String mixinType)
            throws RepositoryException {
        final Node node = getOrAddNode(parent, name);
        node.addMixin(mixinType);
        return node;
    }

    public static String createLabeledVersions(Node node)
            throws RepositoryException, InterruptedException {
        final Session session = node.getSession();
        final VersionManager versionManager = session.getWorkspace().getVersionManager();
        node.setProperty("version", "root");
        session.save();

        final String path = node.getPath();
        final List<String> versionNames = new ArrayList<String>();
        for (final String label : LABELS) {
            node.setProperty("version", label);
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

    public static void assertLabeledVersions(VersionHistory history) throws RepositoryException {
        final VersionIterator versions = history.getAllVersions();
        assertFalse(versions.nextVersion().getFrozenNode().hasProperty("version")); // root
        for (final String label : LABELS) {
            assertEquals(label, versions.nextVersion().getFrozenNode().getProperty("version").getString());
        }
    }

    public interface VersionCopySetup {
        void setup(VersionCopyConfiguration config);
    }
}
