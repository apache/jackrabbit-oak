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
package org.apache.jackrabbit.oak.upgrade.security;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.NT_REP_AUTHORIZABLE_FOLDER;

/**
 * There are occasions where in old JR2 repositories not all ancestors on
 * the users path are of type {@code rep:AuthorizableFolder}, thus leading
 * to exceptions after repository upgrade.
 * <br>
 * In order to avoid such situations, this hook verifies that all nodes on
 * the users and groups paths are of type {@code rep:AuthorizableFolder} and
 * fixes the node-type if it is incorrect.
 */
public class AuthorizableFolderEditor extends DefaultEditor {

    private static final Logger LOG = LoggerFactory.getLogger(AuthorizableFolderEditor.class);

    private final NodeBuilder currentBuilder;

    private final String groupsPath;

    private final String usersPath;

    private final String path;

    public AuthorizableFolderEditor(final NodeBuilder builder, final String path, final String groupsPath, final String usersPath) {
        this.currentBuilder = builder;
        this.groupsPath = groupsPath;
        this.usersPath = usersPath;
        this.path = path;
    }

    public static EditorProvider provider(final String groupsPath, final String usersPath) {
        return new EditorProvider() {
            @Override
            public Editor getRootEditor(final NodeState before,
                                        final NodeState after,
                                        final NodeBuilder builder,
                                        final CommitInfo info) throws CommitFailedException {
                return new AuthorizableFolderEditor(builder, "/", groupsPath, usersPath);
            }
        };
    }

    @Override
    public void propertyAdded(final PropertyState after) throws CommitFailedException {
         propertyChanged(null, after);
    }

    @Override
    public void propertyChanged(final PropertyState before, final PropertyState after) throws CommitFailedException {
        if (JCR_PRIMARYTYPE.equals(after.getName())) {
            String nodeType = after.getValue(Type.STRING);
            LOG.debug("Found {}/jcr:primaryType = {}", path, nodeType);
            if (!nodeType.equals(NT_REP_AUTHORIZABLE_FOLDER) && expectAuthorizableFolder(path)) {
                LOG.info("Changed {}/jcr:primaryType from {} to {}", path, nodeType, NT_REP_AUTHORIZABLE_FOLDER);
                currentBuilder.setProperty(JCR_PRIMARYTYPE, NT_REP_AUTHORIZABLE_FOLDER, Type.NAME);
            }
        }
    }

    private boolean expectAuthorizableFolder(final String path) {
        return !PathUtils.denotesRoot(path)
                && (PathUtils.isAncestor(path, groupsPath) || path.equals(groupsPath)
                || PathUtils.isAncestor(path, usersPath) || path.equals(usersPath));
    }

    @Override
    public Editor childNodeAdded(final String name, final NodeState after) throws CommitFailedException {
        return childNodeChanged(name, null, after);
    }

    @Override
    public Editor childNodeChanged(final String name, final NodeState before, final NodeState after) throws CommitFailedException {
        final String childPath = PathUtils.concat(path, name);
        if (expectAuthorizableFolder(childPath)) {
            LOG.debug("Found {} - descending", childPath);
            return new AuthorizableFolderEditor(currentBuilder.child(name), childPath, groupsPath, usersPath);
        } else {
            return null;
        }
    }
}
