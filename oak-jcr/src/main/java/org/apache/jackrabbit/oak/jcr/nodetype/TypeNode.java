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
package org.apache.jackrabbit.oak.jcr.nodetype;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeManager;

/**
 * Base class for node type, property and node definitions based on
 * in-content definitions.
 */
class TypeNode {

    private final Node node;

    protected TypeNode(Node node) {
        this.node = node;
    }

    protected IllegalStateException illegalState(Exception e) {
        return new IllegalStateException(
                "Unable to access node type information from " + node, e);
    }

    protected NodeType getType(NodeTypeManager manager, String name) {
        try {
            return manager.getNodeType(name);
        } catch (RepositoryException e) {
            throw illegalState(e);
        }
    }

    protected boolean getBoolean(String name) {
        try {
            return node.getProperty(name).getBoolean();
        } catch (RepositoryException e) {
            throw illegalState(e);
        }
    }

    protected String getString(String name) {
        try {
            return node.getProperty(name).getString();
        } catch (RepositoryException e) {
            throw illegalState(e);
        }
    }

    protected String getString(String name, String ifNotFound) {
        try {
            return node.getProperty(name).getString();
        } catch (PathNotFoundException e) {
            return ifNotFound;
        } catch (RepositoryException e) {
            throw illegalState(e);
        }
    }

    protected String[] getStrings(String name) {
        try {
            return getStrings(node.getProperty(name).getValues());
        } catch (RepositoryException e) {
            throw illegalState(e);
        }
    }

    protected String[] getStrings(String name, String[] ifNotFound) {
        try {
            return getStrings(node.getProperty(name).getValues());
        } catch (PathNotFoundException e) {
            return ifNotFound;
        } catch (RepositoryException e) {
            throw illegalState(e);
        }
    }

    protected Value[] getValues(String name, Value[] ifNotFound) {
        try {
            return node.getProperty(name).getValues();
        } catch (PathNotFoundException e) {
            return ifNotFound;
        } catch (RepositoryException e) {
            throw illegalState(e);
        }
    }

    private String[] getStrings(Value[] values) throws RepositoryException {
        String[] strings = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            strings[i] = values[i].getString();
        }
        return strings;
    }

    protected NodeIterator getNodes(String name) {
        try {
            return node.getNodes(name);
        } catch (RepositoryException e) {
            throw illegalState(e);
        }
    }

}
