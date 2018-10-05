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
package org.apache.jackrabbit.oak.spi.security.privilege;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.JcrConstants;

/**
 * Internal name constants used for the privilege management.
 */
public interface PrivilegeConstants {

    //--------------------------------< Constants for Privilege Definitions >---
    /**
     * Internal (oak) name for the root node of the privilege store.
     */
    String REP_PRIVILEGES = "rep:privileges";

    /**
     * Name of the property that defines if the privilege is abstract.
     */
    String REP_IS_ABSTRACT = "rep:isAbstract";

    /**
     * Name of the privilege definition property that stores the aggregate privilege names.
     */
    String REP_AGGREGATES = "rep:aggregates";

    /**
     * Name of the property storing the value of the next available privilege bits.
     */
    String REP_NEXT = "rep:next";

    /**
     * The internal names of all property definitions that are associated with
     * the {@link #NT_REP_PRIVILEGE rep:Privilege} node type
     */
    Set<String> PRIVILEGE_PROPERTY_NAMES = ImmutableSet.of(REP_IS_ABSTRACT, REP_AGGREGATES, REP_NEXT);

    /**
     * Internal (oak) path for the privilege store.
     */
    String PRIVILEGES_PATH = '/' + JcrConstants.JCR_SYSTEM + '/' + REP_PRIVILEGES;

    /**
     * Node type name of the root node of the privilege store
     */
    String NT_REP_PRIVILEGES = "rep:Privileges";

    /**
     * Node type name of the privilege definition nodes
     */
    String NT_REP_PRIVILEGE = "rep:Privilege";

    /**
     * Node type names associated with privilege content
     */
    Set<String> PRIVILEGE_NODETYPE_NAMES = ImmutableSet.of(NT_REP_PRIVILEGES, NT_REP_PRIVILEGE);

    /**
     * Name of the privilege definition property that stores the internal representation
     * of this privilege.
     */
    String REP_BITS = "rep:bits";

    //----------------------------------------------------< Privilege Names >---
    /**
     * Internal (oak) name of the {@link javax.jcr.security.Privilege#JCR_READ} privilege
     */
    String JCR_READ = "jcr:read";

    /**
     * Internal (oak) name of the {@link javax.jcr.security.Privilege#JCR_MODIFY_PROPERTIES} privilege
     */
    String JCR_MODIFY_PROPERTIES = "jcr:modifyProperties";

    /**
     * Internal (oak) name of the {@link javax.jcr.security.Privilege#JCR_ADD_CHILD_NODES} privilege
     */
    String JCR_ADD_CHILD_NODES = "jcr:addChildNodes";

    /**
     * Internal (oak) name of the {@link javax.jcr.security.Privilege#JCR_REMOVE_NODE} privilege
     */
    String JCR_REMOVE_NODE = "jcr:removeNode";

    /**
     * Internal (oak) name of the {@link javax.jcr.security.Privilege#JCR_REMOVE_CHILD_NODES} privilege
     */
    String JCR_REMOVE_CHILD_NODES = "jcr:removeChildNodes";

    /**
     * Internal (oak) name of the {@link javax.jcr.security.Privilege#JCR_WRITE} privilege
     */
    String JCR_WRITE = "jcr:write";

    /**
     * Internal (oak) name of the {@link javax.jcr.security.Privilege#JCR_READ_ACCESS_CONTROL} privilege
     */
    String JCR_READ_ACCESS_CONTROL = "jcr:readAccessControl";

    /**
     * Internal (oak) name of the {@link javax.jcr.security.Privilege#JCR_MODIFY_ACCESS_CONTROL} privilege
     */
    String JCR_MODIFY_ACCESS_CONTROL = "jcr:modifyAccessControl";

    /**
     * Internal (oak) name of the {@link javax.jcr.security.Privilege#JCR_LOCK_MANAGEMENT} privilege
     */
    String JCR_LOCK_MANAGEMENT = "jcr:lockManagement";

    /**
     * Internal (oak) name of the {@link javax.jcr.security.Privilege#JCR_VERSION_MANAGEMENT} privilege
     */
    String JCR_VERSION_MANAGEMENT = "jcr:versionManagement";

    /**
     * Internal (oak) name of the {@link javax.jcr.security.Privilege#JCR_NODE_TYPE_MANAGEMENT} privilege
     */
    String JCR_NODE_TYPE_MANAGEMENT = "jcr:nodeTypeManagement";

    /**
     * Internal (oak) name of the {@link javax.jcr.security.Privilege#JCR_RETENTION_MANAGEMENT} privilege
     */
    String JCR_RETENTION_MANAGEMENT = "jcr:retentionManagement";

    /**
     * Internal (oak) name of the {@link javax.jcr.security.Privilege#JCR_LIFECYCLE_MANAGEMENT} privilege
     */
    String JCR_LIFECYCLE_MANAGEMENT = "jcr:lifecycleManagement";

    /**
     * Internal (oak) name of the jcr:workspaceManagement privilege
     */
    String JCR_WORKSPACE_MANAGEMENT = "jcr:workspaceManagement";

    /**
     * Internal (oak) name of the jcr:nodeTypeDefinitionManagement privilege
     */
    String JCR_NODE_TYPE_DEFINITION_MANAGEMENT = "jcr:nodeTypeDefinitionManagement";

    /**
     * Internal (oak) name of the jcr:namespaceManagement privilege
     */
    String JCR_NAMESPACE_MANAGEMENT = "jcr:namespaceManagement";

    /**
     * Internal (oak) name of the {@link javax.jcr.security.Privilege#JCR_ALL} privilege
     */
    String JCR_ALL = "jcr:all";

    /**
     * Internal (oak) name of the rep:privilegeManagement privilege
     */
    String REP_PRIVILEGE_MANAGEMENT = "rep:privilegeManagement";

    /**
     * Internal (oak) name of the rep:write privilege
     */
    String REP_WRITE = "rep:write";

    /**
     * Internal (oak) name of the rep:userManagement privilege
     *
     * @since OAK 1.0
     */
    String REP_USER_MANAGEMENT = "rep:userManagement";

    /**
     * Internal (oak) name of the rep:readNodes privilege
     *
     * @since OAK 1.0
     */
    String REP_READ_NODES = "rep:readNodes";

    /**
     * Internal (oak) name of the rep:readProperties privilege
     *
     * @since OAK 1.0
     */
    String REP_READ_PROPERTIES = "rep:readProperties";

    /**
     * Internal (oak) name of the rep:addProperties privilege
     *
     * @since OAK 1.0
     */
    String REP_ADD_PROPERTIES = "rep:addProperties";

    /**
     * Internal (oak) name of the rep:alterProperties privilege
     *
     * @since OAK 1.0
     */
    String REP_ALTER_PROPERTIES = "rep:alterProperties";

    /**
     * Internal (oak) name of the rep:removeProperties privilege
     *
     * @since OAK 1.0
     */
    String REP_REMOVE_PROPERTIES = "rep:removeProperties";

    /**
     * Internal (oak) name of the rep:indexDefinitionManagement privilege
     *
     * @since OAK 1.0
     */
    String REP_INDEX_DEFINITION_MANAGEMENT = "rep:indexDefinitionManagement";

    /**
     * The internal names of all built-in privileges that are not aggregates.
     */
    Set<String> NON_AGGREGATE_PRIVILEGES = ImmutableSet.of(
            REP_READ_NODES, REP_READ_PROPERTIES,
                        REP_ADD_PROPERTIES, REP_ALTER_PROPERTIES, REP_REMOVE_PROPERTIES,
                        JCR_ADD_CHILD_NODES, JCR_REMOVE_CHILD_NODES, JCR_REMOVE_NODE,
                        JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL, JCR_NODE_TYPE_MANAGEMENT,
                        JCR_VERSION_MANAGEMENT, JCR_LOCK_MANAGEMENT, JCR_LIFECYCLE_MANAGEMENT,
                        JCR_RETENTION_MANAGEMENT, JCR_WORKSPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT,
                        JCR_NAMESPACE_MANAGEMENT, REP_PRIVILEGE_MANAGEMENT, REP_USER_MANAGEMENT, REP_INDEX_DEFINITION_MANAGEMENT

    );

    /**
     * The internal names and aggregation definition of all built-in privileges
     * that are aggregates (except for jcr:all).
     */
    Map<String, String[]> AGGREGATE_PRIVILEGES = ImmutableMap.of(
            JCR_READ, new String[]{REP_READ_NODES, REP_READ_PROPERTIES},
            JCR_MODIFY_PROPERTIES, new String[]{REP_ADD_PROPERTIES, REP_ALTER_PROPERTIES, REP_REMOVE_PROPERTIES},
            JCR_WRITE, new String[]{JCR_MODIFY_PROPERTIES, JCR_ADD_CHILD_NODES, JCR_REMOVE_CHILD_NODES, JCR_REMOVE_NODE},
            REP_WRITE, new String[]{JCR_WRITE, JCR_NODE_TYPE_MANAGEMENT});
}
