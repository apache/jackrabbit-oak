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

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.remote.RemoteCommitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class UnsetContentRemoteOperation implements ContentRemoteOperation {

    private static final Logger logger = LoggerFactory.getLogger(UnsetContentRemoteOperation.class);

    private final String path;

    private final String name;

    public UnsetContentRemoteOperation(String path, String name) {
        this.path = path;
        this.name = name;
    }

    @Override
    public void apply(Root root) throws RemoteCommitException {
        logger.debug("performing 'unset' operation on path={}, name={}", path, name);

        Tree tree = root.getTree(path);

        if (!tree.exists()) {
            throw new RemoteCommitException("tree does not exists");
        }

        if (!tree.hasProperty(name)) {
            throw new RemoteCommitException("property does not exist");
        }

        tree.removeProperty(name);
    }

}
