/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.version;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.JcrConstants.JCR_ROOTVERSION;
import static org.apache.jackrabbit.oak.api.CommitFailedException.CONSTRAINT;
import static org.apache.jackrabbit.oak.plugins.version.Utils.throwProtected;

/**
 * Validates changes on jcr:versionLabels nodes in the version storage.
 */
class VersionLabelsEditor extends DefaultEditor {

    private final String path;
    private final ReadWriteVersionManager vMgr;

    VersionLabelsEditor(String labelsPath,
                        ReadWriteVersionManager versionManager) {
        this.path = labelsPath;
        this.vMgr = versionManager;
    }

    @Override
    public void propertyAdded(PropertyState after)
            throws CommitFailedException {
        validateLabel(after);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        validateLabel(after);
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after)
            throws CommitFailedException {
        return throwProtected(PathUtils.concat(path, name));
    }

    @Override
    public Editor childNodeChanged(String name,
                                   NodeState before,
                                   NodeState after)
            throws CommitFailedException {
        return throwProtected(PathUtils.concat(path, name));
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        return throwProtected(PathUtils.concat(path, name));
    }

    //-----------------------< internal >---------------------------------------

    private void validateLabel(PropertyState label)
            throws CommitFailedException {
        String identifier = label.getValue(Type.REFERENCE);
        Tree version = vMgr.getVersion(identifier);
        if (version == null) {
            throw new CommitFailedException(CONSTRAINT, 0,
                    "Version label references unknown node");
        }
        String parent = PathUtils.getAncestorPath(path, 1);
        String versionName = version.getName();
        if (versionName.equals(JCR_ROOTVERSION)
                || !PathUtils.isAncestor(parent, version.getPath())) {
            throw new CommitFailedException(CommitFailedException.VERSION,
                    VersionExceptionCode.NO_SUCH_VERSION.ordinal(),
                    "Not a valid version on this history: " + versionName);
        }
    }
}
