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
package org.apache.jackrabbit.oak.jcr.security.user;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;

/**
 * NodeUtil...
 *
 * FIXME: remove again.
 * FIXME: this is a tmp workaround for missing conversion from Node to Tree
 */
class NodeTreeUtil {

    private final Session session;
    private final Root root;
    private final NamePathMapper mapper;

    NodeTreeUtil(Session session, Root root, NamePathMapper mapper) {
        this.session = session;
        this.root = root;
        this.mapper = mapper;
    }

    Tree getTree(Node node) throws RepositoryException {
        return root.getTree(mapper.getOakPath(node.getPath()));
    }

    Node getNode(Tree tree) throws RepositoryException {
        return session.getNode(mapper.getJcrPath(tree.getPath()));
    }
}