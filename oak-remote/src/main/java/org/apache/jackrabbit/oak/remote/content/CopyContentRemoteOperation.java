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

package org.apache.jackrabbit.oak.remote.content;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.remote.RemoteCommitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CopyContentRemoteOperation implements ContentRemoteOperation {

    private static final Logger logger = LoggerFactory.getLogger(CopyContentRemoteOperation.class);

    private final String source;

    private final String target;

    public CopyContentRemoteOperation(String source, String target) {
        this.source = source;
        this.target = target;
    }

    @Override
    public void apply(Root root) throws RemoteCommitException {
        logger.debug("performing 'copy' operation on source={}, target={}", source, target);

        Tree sourceTree = root.getTree(source);

        if (!sourceTree.exists()) {
            throw new RemoteCommitException("source tree does not exist");
        }

        Tree targetTree = root.getTree(target);

        if (targetTree.exists()) {
            throw new RemoteCommitException("target tree already exists");
        }

        Tree targetParentTree = targetTree.getParent();

        if (!targetParentTree.exists()) {
            throw new RemoteCommitException("parent of target tree does not exist");
        }

        copy(sourceTree, targetParentTree, targetTree.getName());
    }

    private void copy(Tree source, Tree targetParent, String targetName) {
        Tree target = targetParent.addChild(targetName);

        for (PropertyState property : source.getProperties()) {
            target.setProperty(property);
        }

        for (Tree child : source.getChildren()) {
            copy(child, target, child.getName());
        }
    }

}
