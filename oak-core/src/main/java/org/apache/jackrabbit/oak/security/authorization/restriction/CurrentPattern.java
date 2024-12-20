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
package org.apache.jackrabbit.oak.security.authorization.restriction;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.jcr.NamespaceRegistry;

import java.util.Objects;
import java.util.Set;

/**
 * Restriction that limits the effect of a given ACE to the target node where the entry takes effect and optionally it's
 * properties (or a subset thereof).
 * 
 * The following values are allowed for the corresponding multi-valued property:
 * <table>
 * <tr><td>empty value array</td><td>restriction applies to the target node only, properties are never included</td></tr>    
 * <tr><td>value with {@link NodeTypeConstants#RESIDUAL_NAME residual name '*'}</td><td>restriction applies to the target node and all it's properties</td></tr>    
 * <tr><td>one or multiple property names</td><td>restriction applies to the target node and the specified properties</td></tr>
 * </table>
 */
class CurrentPattern implements RestrictionPattern {

    /**
     * Built-in namespace prefixes
     */
    private static final Set<String> PREFIXES = Set.of(
            NamespaceConstants.PREFIX_OAK, 
            NamespaceConstants.PREFIX_REP,
            NamespaceRegistry.PREFIX_JCR);

    /**
     * Known names of nodes defined by built-in node type definitions, which allows for a best-effort estimate if a given
     * name with one of the built-in namespace prefixes is a node or a property.
     * 
     * NOTE: {@link UserConstants#REP_MEMBERS} and {@link org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants#REP_PRIVILEGES}
     * are ambiguous as they are defined for both node and property definitions. however, {@link UserConstants#REP_MEMBERS} 
     * child node definition is deprecated and no longer used in Oak. {@link org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants#REP_PRIVILEGES}
     * is used for a single node below jcr:system only, while the property name is used in every access control entry.
     * Therefore these two names are omitted from the list.
     */
    private static final Set<String> NODE_NAMES = Set.of(
            JcrConstants.JCR_CHILDNODEDEFINITION, 
            JcrConstants.JCR_CONTENT, 
            JcrConstants.JCR_FROZENNODE, 
            JcrConstants.JCR_PROPERTYDEFINITION, 
            JcrConstants.JCR_ROOTVERSION,
            JcrConstants.JCR_SYSTEM, 
            JcrConstants.JCR_VERSIONLABELS,
            JcrConstants.JCR_VERSIONSTORAGE,
            NodeTypeConstants.JCR_NODE_TYPES,
            NodeTypeConstants.REP_NAMED_CHILD_NODE_DEFINITIONS,
            NodeTypeConstants.REP_RESIDUAL_CHILD_NODE_DEFINITIONS,
            NodeTypeConstants.REP_NAMED_PROPERTY_DEFINITIONS,
            NodeTypeConstants.REP_RESIDUAL_PROPERTY_DEFINITIONS,
            NodeTypeConstants.REP_OURS,
            VersionConstants.JCR_ACTIVITIES,
            VersionConstants.JCR_CONFIGURATIONS,
            AccessControlConstants.REP_POLICY,
            AccessControlConstants.REP_REPO_POLICY,
            AccessControlConstants.REP_RESTRICTIONS,
            UserConstants.REP_PWD,
            UserConstants.REP_MEMBERS_LIST,
            IndexConstants.INDEX_DEFINITIONS_NAME,
            "rep:cugPolicy",
            "rep:principalPolicy");
    
    private final String treePath;
    private final Set<String> propertyNames;

    CurrentPattern(@NotNull String treePath, @NotNull Iterable<String> propertyNames) {
        this.treePath = treePath;
        this.propertyNames = CollectionUtils.toSet(propertyNames);
    }

    @Override
    public boolean matches(@NotNull Tree tree, @Nullable PropertyState property) {
        String propName = (property == null) ? null : property.getName();
        return matches(tree.getPath(), propName);
    }

    @Override
    public boolean matches(@NotNull String path) {
        // best-effort attempt to determine if the specified path points to a property. if it cannot be determined,
        // assume that it points ot a node.
        String propName = getPropertyNameOrNull(path);
        String nodePath = (propName == null) ? path : PathUtils.getParentPath(path);
        return matches(nodePath, propName);
    }

    @Override
    public boolean matches(@NotNull String path, boolean isProperty) {
        if (isProperty) {
            if (PathUtils.denotesRoot(path)) {
                return false;
            }
            return matches(PathUtils.getParentPath(path),  PathUtils.getName(path));
        } else {
            return matches(path, null);
        }
    }

    @Override
    public boolean matches() {
        // pattern never matches for repository level permissions
        return false;
    }
    
    private boolean matches(@NotNull String nodePath, @Nullable String propertyName) {
        if (!this.treePath.equals(nodePath)) {
            return false;
        }
        if (propertyName == null) {
            // no property name to compare
            return true;
        } else {
            // restriction needs to be evaluated for a property
            if (propertyNames.isEmpty()) {
                // only node itself matches
                return false;
            } else if (propertyNames.contains(NodeTypeConstants.RESIDUAL_NAME)) {
                // the node always matches and all properties match if the given name-set is empty
                return true;
            } else {
                // verify that given propName is explicitly part of the restriction
                return propertyNames.contains(propertyName);
            }
        }
    }

    /**
     * Best-effort attempt to determine if the specified path points to a property or not. If it cannot be determined,
     * this method returns {@code null} assuming that the path points to a node.
     * 
     * @param path The path as passed to {@link #matches(String)}
     * @return A non-null string if the given path ends with a name that is known to belong to a property defined by 
     * a named property definition of a built-in node type. It returns {@code null} if the name either belongs to a 
     * named child-node definition of a built-in node type or if it is not possible to determined if the given path 
     * points to a property.
     */
    @Nullable
    private static String getPropertyNameOrNull(@NotNull String path) {
        if (PathUtils.denotesRoot(path)) {
            return null;
        }
        String name = PathUtils.getName(path);
        String prefix = Text.getNamespacePrefix(name);
        if (PREFIXES.contains(prefix) && !NODE_NAMES.contains(name)) {
            return name;
        } else {
            return null;
        }
    }

    //-------------------------------------------------------------< Object >---
    @Override
    public int hashCode() {
        return Objects.hash(treePath, propertyNames);
    }

    @Override
    public String toString() {
        return treePath + " : " + propertyNames;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof CurrentPattern) {
            CurrentPattern other = (CurrentPattern) obj;
            return treePath.equals(other.treePath) &&  propertyNames.equals(other.propertyNames);
        }
        return false;
    }
}