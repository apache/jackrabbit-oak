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
package org.apache.jackrabbit.oak.plugins.nodetype;

import javax.annotation.Nonnull;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.PropertyDefinition;

/**
 * DefinitionProvider... TODO
 */
public interface DefinitionProvider {

    @Nonnull
    NodeDefinition getRootDefinition() throws RepositoryException;

    @Nonnull
    NodeDefinition getDefinition(Node parent, Node targetNode) throws RepositoryException;

    @Nonnull
    PropertyDefinition getDefinition(Node parent, Property targetProperty) throws RepositoryException;

    @Nonnull
    PropertyDefinition getDefinition(Node parent, String propertyName, boolean isMultiple, int type, boolean exactTypeMatch) throws RepositoryException;

    @Nonnull
    PropertyDefinition getDefinition(NodeType nodeType, String propertyName, boolean isMultiple, int type, boolean exactTypeMatch) throws RepositoryException;
}