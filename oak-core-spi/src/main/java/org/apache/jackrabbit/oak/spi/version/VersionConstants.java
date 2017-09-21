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
package org.apache.jackrabbit.oak.spi.version;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.JcrConstants;

/**
 * VersionConstants... TODO
 */
public interface VersionConstants extends JcrConstants {

    // version storage
    String REP_VERSIONSTORAGE = "rep:versionStorage";

    // activities
    String JCR_ACTIVITY = "jcr:activity";
    String JCR_ACTIVITIES = "jcr:activities";
    String JCR_ACTIVITY_TITLE = "jcr:activityTitle";
    String NT_ACTIVITY = "nt:activity";
    String REP_ACTIVITIES = "rep:Activities";

    // configurations
    String JCR_CONFIGURATION = "jcr:configuration";
    String JCR_CONFIGURATIONS = "jcr:configurations";
    String JCR_ROOT = "jcr:root"; // TODO: possible collisions?
    String NT_CONFIGURATION = "nt:configuration";
    String REP_CONFIGURATIONS = "rep:Configurations";

    // nt:versionHistory
    String JCR_COPIED_FROM = "jcr:copiedFrom";

    // nt:versionedChild
    String JCR_CHILD_VERSION_HISTORY = "jcr:childVersionHistory";

    /**
     * @since OAK 1.0
     */
    String MIX_REP_VERSIONABLE_PATHS = "rep:VersionablePaths";

    /**
     * Prefix of the jcr:baseVersion value for a restore.
     */
    String RESTORE_PREFIX = "restore-";

    /**
     * Quote from JSR 283 Section "15.12.3 Activity Storage"<p>
     * <p>
     * Activities are persisted as nodes of type nt:activity under system-generated
     * node names in activity storage below /jcr:system/jcr:activities.<br>
     * Similar to the /jcr:system/jcr:versionStorage subgraph, the activity storage
     * is a single repository wide store, but is reflected into each workspace.
     */
    String ACTIVITIES_PATH = '/' + JCR_SYSTEM + '/' + JCR_ACTIVITIES;

    /**
     * Quote from JSR 283 Section "15.13.2 Configuration Proxy Nodes"<p>
     * <p>
     * Each configuration in a given workspace is represented by a distinct proxy
     * node of type nt:configuration located in configuration storage within the
     * same workspace under /jcr:system/jcr:configurations/. The configuration
     * storage in a particular workspace is specific to that workspace. It is
     * not a common repository-wide store mirrored into each workspace, as is
     * the case with version storage.
     */
    String CONFIGURATIONS_PATH = '/' + JCR_SYSTEM + '/' + JCR_CONFIGURATIONS;

    /**
     * Quote from JSR 283 Section "3.13.8 Version Storage"<p>
     * <p>
     * Version histories are stored in a single, repository-wide version storage
     * mutable and readable through the versioning API.
     * Under full versioning the version storage data must, additionally, be
     * reflected in each workspace as a protected subgraph [...] located below
     * /jcr:system/jcr:versionStorage.
     */
    String VERSION_STORE_PATH = '/' + JCR_SYSTEM + '/' + JCR_VERSIONSTORAGE;

    Collection<String> SYSTEM_PATHS = Collections.unmodifiableList(Arrays.asList(
            ACTIVITIES_PATH,
            CONFIGURATIONS_PATH,
            VERSION_STORE_PATH
    ));

    Collection<String> VERSION_PROPERTY_NAMES = Collections.unmodifiableList(Arrays.asList(
            JCR_ACTIVITY,
            JCR_ACTIVITY_TITLE,
            JCR_BASEVERSION,
            JCR_CHILD_VERSION_HISTORY,
            JCR_CONFIGURATION,
            JCR_COPIED_FROM,
            JCR_FROZENMIXINTYPES,
            JCR_FROZENPRIMARYTYPE,
            JCR_FROZENUUID,
            JCR_ISCHECKEDOUT,
            JCR_MERGEFAILED,
            JCR_PREDECESSORS,
            JCR_ROOT,
            JCR_SUCCESSORS,
            JCR_VERSIONABLEUUID,
            JCR_VERSIONHISTORY
    ));

    Collection<String> VERSION_NODE_NAMES = Collections.unmodifiableList(Arrays.asList(
            JCR_ACTIVITIES,
            JCR_CONFIGURATIONS,
            JCR_FROZENNODE,
            JCR_ROOTVERSION,
            JCR_VERSIONLABELS
    ));

    Collection<String> VERSION_NODE_TYPE_NAMES = Collections.unmodifiableList(Arrays.asList(
            NT_ACTIVITY,
            NT_CONFIGURATION,
            NT_FROZENNODE,
            NT_VERSION,
            NT_VERSIONEDCHILD,
            NT_VERSIONHISTORY,
            NT_VERSIONLABELS,
            REP_ACTIVITIES,
            REP_CONFIGURATIONS
    ));

    Set<String> VERSION_STORE_ROOT_NAMES = ImmutableSet.of(
            JcrConstants.JCR_VERSIONSTORAGE,
            VersionConstants.JCR_CONFIGURATIONS,
            VersionConstants.JCR_ACTIVITIES);

    Set<String> VERSION_STORE_NT_NAMES = ImmutableSet.of(
            VersionConstants.REP_VERSIONSTORAGE,
            VersionConstants.REP_ACTIVITIES,
            VersionConstants.REP_CONFIGURATIONS
    );
}