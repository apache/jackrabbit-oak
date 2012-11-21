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
package org.apache.jackrabbit.oak.security.privilege;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.JcrConstants;

/**
 * PrivilegeConstants... TODO
 */
public interface PrivilegeConstants {

    // constants for privilege serialization
    String REP_PRIVILEGES = "rep:privileges";
    String PRIVILEGES_PATH = '/' + JcrConstants.JCR_SYSTEM + '/' + REP_PRIVILEGES;

    String NT_REP_PRIVILEGES = "rep:Privileges";
    String NT_REP_PRIVILEGE = "rep:Privilege";

    String REP_IS_ABSTRACT = "rep:isAbstract";
    String REP_AGGREGATES = "rep:aggregates";

    // Constants for privilege names
    String JCR_READ = "jcr:read";
    String JCR_MODIFY_PROPERTIES = "jcr:modifyProperties";
    String JCR_ADD_CHILD_NODES = "jcr:addChildNodes";
    String JCR_REMOVE_NODE = "jcr:removeNode";
    String JCR_REMOVE_CHILD_NODES = "jcr:removeChildNodes";
    String JCR_WRITE = "jcr:write";
    String JCR_READ_ACCESS_CONTROL = "jcr:readAccessControl";
    String JCR_MODIFY_ACCESS_CONTROL = "jcr:modifyAccessControl";
    String JCR_LOCK_MANAGEMENT = "jcr:lockManagement";
    String JCR_VERSION_MANAGEMENT = "jcr:versionManagement";
    String JCR_NODE_TYPE_MANAGEMENT = "jcr:nodeTypeManagement";
    String JCR_RETENTION_MANAGEMENT = "jcr:retentionManagement";
    String JCR_LIFECYCLE_MANAGEMENT = "jcr:lifecycleManagement";
    String JCR_WORKSPACE_MANAGEMENT = "jcr:workspaceManagement";
    String JCR_NODE_TYPE_DEFINITION_MANAGEMENT = "jcr:nodeTypeDefinitionManagement";
    String JCR_NAMESPACE_MANAGEMENT = "jcr:namespaceManagement";
    String JCR_ALL = "jcr:all";

    String REP_PRIVILEGE_MANAGEMENT = "rep:privilegeManagement";
    String REP_USER_MANAGEMENT = "rep:userManagement";
    String REP_WRITE = "rep:write";
    String REP_READ_NODES = "rep:readNodes";
    String REP_READ_PROPERTIES = "rep:readProperties";
    String REP_ADD_PROPERTIES = "rep:addProperties";
    String REP_ALTER_PROPERTIES = "rep:alterProperties";
    String REP_REMOVE_PROPERTIES = "rep:removeProperties";

    String[] NON_AGGR_PRIVILEGES = new String[] {
            REP_READ_NODES, REP_READ_PROPERTIES,
            REP_ADD_PROPERTIES, REP_ALTER_PROPERTIES, REP_REMOVE_PROPERTIES,
            JCR_ADD_CHILD_NODES, JCR_REMOVE_CHILD_NODES, JCR_REMOVE_NODE,
            JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL, JCR_NODE_TYPE_MANAGEMENT,
            JCR_VERSION_MANAGEMENT, JCR_LOCK_MANAGEMENT, JCR_LIFECYCLE_MANAGEMENT,
            JCR_RETENTION_MANAGEMENT, JCR_WORKSPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT,
            JCR_NAMESPACE_MANAGEMENT, REP_PRIVILEGE_MANAGEMENT, REP_USER_MANAGEMENT};

    String[] AGGR_JCR_READ = new String[] {
            REP_READ_NODES, REP_READ_PROPERTIES
    };

    String[] AGGR_JCR_MODIFY_PROPERTIES = new String[] {
            REP_ADD_PROPERTIES, REP_ALTER_PROPERTIES, REP_REMOVE_PROPERTIES
    };

    String[] AGGR_JCR_WRITE = new String[] {
            JCR_MODIFY_PROPERTIES, JCR_ADD_CHILD_NODES, JCR_REMOVE_CHILD_NODES, JCR_REMOVE_NODE
    };

    String[] AGGR_REP_WRITE = new String[] {
            JCR_WRITE, JCR_NODE_TYPE_MANAGEMENT
    };

    Map<String, String[]> AGGREGATE_PRIVILEGES = ImmutableMap.of(JCR_READ, AGGR_JCR_READ,
            JCR_MODIFY_PROPERTIES, AGGR_JCR_MODIFY_PROPERTIES,
            JCR_WRITE, AGGR_JCR_WRITE,
            REP_WRITE, AGGR_REP_WRITE);

    Set<String> PRIVILEGE_PROPERTY_NAMES = ImmutableSet.of(REP_IS_ABSTRACT, REP_AGGREGATES);
}