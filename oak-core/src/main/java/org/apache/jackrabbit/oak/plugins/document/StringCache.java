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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.Map;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.spi.state.ConflictType;

/**
 * TODO document
 */
public final class StringCache {

    private static final Map<String, String> CONSTANTS = createStringMap(
            JcrConstants.JCR_AUTOCREATED,
            JcrConstants.JCR_BASEVERSION,
            JcrConstants.JCR_CHILD,
            JcrConstants.JCR_CHILDNODEDEFINITION,
            JcrConstants.JCR_CONTENT,
            JcrConstants.JCR_CREATED,
            JcrConstants.JCR_DATA,
            JcrConstants.JCR_DEFAULTPRIMARYTYPE,
            JcrConstants.JCR_DEFAULTVALUES,
            JcrConstants.JCR_ENCODING,
            JcrConstants.JCR_FROZENMIXINTYPES,
            JcrConstants.JCR_FROZENNODE,
            JcrConstants.JCR_FROZENPRIMARYTYPE,
            JcrConstants.JCR_FROZENUUID,
            JcrConstants.JCR_HASORDERABLECHILDNODES,
            JcrConstants.JCR_ISCHECKEDOUT,
            JcrConstants.JCR_ISMIXIN,
            JcrConstants.JCR_LANGUAGE,
            JcrConstants.JCR_LASTMODIFIED,
            JcrConstants.JCR_LOCKISDEEP,
            JcrConstants.JCR_LOCKOWNER,
            JcrConstants.JCR_MANDATORY,
            JcrConstants.JCR_MERGEFAILED,
            JcrConstants.JCR_MIMETYPE,
            JcrConstants.JCR_MIXINTYPES,
            JcrConstants.JCR_MULTIPLE,
            JcrConstants.JCR_NAME,
            JcrConstants.JCR_NODETYPENAME,
            JcrConstants.JCR_ONPARENTVERSION,
            JcrConstants.JCR_PREDECESSORS,
            JcrConstants.JCR_PRIMARYITEMNAME,
            JcrConstants.JCR_PRIMARYTYPE,
            JcrConstants.JCR_PROPERTYDEFINITION,
            JcrConstants.JCR_PROTECTED,
            JcrConstants.JCR_REQUIREDPRIMARYTYPES,
            JcrConstants.JCR_REQUIREDTYPE,
            JcrConstants.JCR_ROOTVERSION,
            JcrConstants.JCR_SAMENAMESIBLINGS,
            JcrConstants.JCR_STATEMENT,
            JcrConstants.JCR_SUCCESSORS,
            JcrConstants.JCR_SUPERTYPES,
            JcrConstants.JCR_SYSTEM,
            JcrConstants.JCR_UUID,
            JcrConstants.JCR_VALUECONSTRAINTS,
            JcrConstants.JCR_VERSIONHISTORY,
            JcrConstants.JCR_VERSIONLABELS,
            JcrConstants.JCR_VERSIONSTORAGE,
            JcrConstants.JCR_VERSIONABLEUUID,
            JcrConstants.JCR_PATH,
            JcrConstants.JCR_SCORE,
            JcrConstants.MIX_LOCKABLE,
            JcrConstants.MIX_REFERENCEABLE,
            JcrConstants.MIX_VERSIONABLE,
            JcrConstants.MIX_SHAREABLE,
            JcrConstants.NT_BASE,
            JcrConstants.NT_CHILDNODEDEFINITION,
            JcrConstants.NT_FILE,
            JcrConstants.NT_FOLDER,
            JcrConstants.NT_FROZENNODE,
            JcrConstants.NT_HIERARCHYNODE,
            JcrConstants.NT_LINKEDFILE,
            JcrConstants.NT_NODETYPE,
            JcrConstants.NT_PROPERTYDEFINITION,
            JcrConstants.NT_QUERY,
            JcrConstants.NT_RESOURCE,
            JcrConstants.NT_UNSTRUCTURED,
            JcrConstants.NT_VERSION,
            JcrConstants.NT_VERSIONHISTORY,
            JcrConstants.NT_VERSIONLABELS,
            JcrConstants.NT_VERSIONEDCHILD,
            NodeTypeConstants.JCR_NODE_TYPES,
            NodeTypeConstants.JCR_IS_ABSTRACT,
            NodeTypeConstants.JCR_IS_QUERYABLE,
            NodeTypeConstants.JCR_IS_FULLTEXT_SEARCHABLE,
            NodeTypeConstants.JCR_IS_QUERY_ORDERABLE,
            NodeTypeConstants.JCR_AVAILABLE_QUERY_OPERATORS,
            NodeTypeConstants.NT_REP_ROOT,
            NodeTypeConstants.NT_REP_SYSTEM,
            NodeTypeConstants.JCR_CREATEDBY,
            NodeTypeConstants.JCR_LASTMODIFIEDBY,
            NodeTypeConstants.MIX_CREATED,
            NodeTypeConstants.MIX_LASTMODIFIED,
            NodeTypeConstants.MIX_REP_MERGE_CONFLICT,
            NodeTypeConstants.REP_OURS,
            ConflictType.DELETE_CHANGED_PROPERTY.getName(),
            ConflictType.DELETE_CHANGED_NODE.getName(),
            ConflictType.ADD_EXISTING_PROPERTY.getName(),
            ConflictType.CHANGE_DELETED_PROPERTY.getName(),
            ConflictType.CHANGE_CHANGED_PROPERTY.getName(),
            ConflictType.DELETE_DELETED_PROPERTY.getName(),
            ConflictType.ADD_EXISTING_NODE.getName(),
            ConflictType.CHANGE_DELETED_NODE.getName(),
            ConflictType.DELETE_DELETED_NODE.getName(),
            VersionConstants.JCR_ACTIVITY,
            VersionConstants.JCR_ACTIVITIES,
            VersionConstants.JCR_ACTIVITY_TITLE,
            VersionConstants.NT_ACTIVITY,
            VersionConstants.REP_ACTIVITIES,
            VersionConstants.JCR_CONFIGURATION,
            VersionConstants.JCR_CONFIGURATIONS,
            VersionConstants.JCR_ROOT,
            VersionConstants.NT_CONFIGURATION,
            VersionConstants.REP_CONFIGURATIONS);

    private StringCache() {
    }

    private static Map<String, String> createStringMap(String... strings) {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (String string : strings) {
            builder.put(string, string);
        }
        return builder.build();
    }

    // must be a power of 2
    private static final int STRING_CACHE_SIZE = 1024;

    private static final String[] STRING_CACHE = new String[STRING_CACHE_SIZE];

    public static String get(String s) {
        String constant = CONSTANTS.get(s);
        if (constant != null) {
            return constant;
        }

        int index = s.hashCode() & (STRING_CACHE_SIZE - 1);
        String cached = STRING_CACHE[index];
        if (!s.equals(cached)) {
            cached = new String(s); // avoid referring to 
            STRING_CACHE[index] = cached;
        }
        return cached;
    }

}
