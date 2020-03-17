/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.jackrabbit.oak.scalability.util;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.nodetype.NodeDefinitionTemplate;
import javax.jcr.nodetype.NodeTypeDefinition;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinitionTemplate;
import javax.jcr.version.OnParentVersionAction;

/**
 * Helper class for creating node types.
 */
public class NodeTypeUtils {

    /**
     * Creates a node type with the given properties.
     *
     * @param session the session
     * @param name the name
     * @param properties the properties
     * @param superTypes the super types
     * @param childrenTypes the children types
     * @param baseType the base type
     * @param isMixin the is mixin
     * @return the string
     * @throws RepositoryException the repository exception
     */
    @SuppressWarnings("unchecked")
    public static String createNodeType(Session session, String name,
            String[] properties, int propTypes[], String[] superTypes, String[] childrenTypes,
            String baseType, boolean isMixin)
            throws RepositoryException {
        NodeTypeManager ntm = session.getWorkspace().getNodeTypeManager();
        NodeTypeTemplate ntt = ntm.createNodeTypeTemplate();
        if (baseType != null) {
            NodeTypeDefinition ntd = ntm.getNodeType(baseType);
            ntt = ntm.createNodeTypeTemplate(ntd);
        }

        if ((superTypes != null) && (superTypes.length != 0)) {
            ntt.setDeclaredSuperTypeNames(superTypes);
        }
        ntt.setOrderableChildNodes(false);
        ntt.setName(name);

        if (properties != null) {
            for (int count = 0; count < properties.length; count++) {
                ntt.getPropertyDefinitionTemplates().add(
                        createPropertyDefTemplate(ntm, properties[count], propTypes[count]));
            }
        }

        if (childrenTypes != null) {
            ntt.getNodeDefinitionTemplates().add(
                    createNodeDefTemplate(ntm, childrenTypes));
        }

        ntt.setMixin(isMixin);

        ntm.registerNodeType(ntt, true);
        return ntt.getName();
    }

    /**
     * Creates the property definition template.
     *
     * @param ntm the ntm
     * @param prop the prop
     * @return the property definition template
     * @throws RepositoryException the repository exception
     */
    private static PropertyDefinitionTemplate createPropertyDefTemplate(NodeTypeManager ntm,
            String prop, int type) throws RepositoryException {
        PropertyDefinitionTemplate pdt = ntm.createPropertyDefinitionTemplate();
        pdt.setName(prop);
        pdt.setOnParentVersion(OnParentVersionAction.IGNORE);
        pdt.setRequiredType(type);
        pdt.setValueConstraints(null);
        pdt.setDefaultValues(null);
        pdt.setFullTextSearchable(true);
        pdt.setValueConstraints(new String[0]);
        return pdt;
    }

    /**
     * Creates the node definition template.
     *
     * @param ntm the ntm
     * @param types the types
     * @return the node definition template
     * @throws RepositoryException the repository exception
     */
    private static NodeDefinitionTemplate createNodeDefTemplate(NodeTypeManager ntm, String[]
        types) throws RepositoryException {
        NodeDefinitionTemplate ndt = ntm.createNodeDefinitionTemplate();
        ndt.setOnParentVersion(OnParentVersionAction.IGNORE);
        ndt.setRequiredPrimaryTypeNames(types);
        ndt.setDefaultPrimaryTypeName(types[0]);
        ndt.setName("*");
        return ndt;
    }
}

