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
package org.apache.jackrabbit.oak.exercise.security.authorization;

import javax.jcr.security.AccessControlManager;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;

/**
 * <pre>
 * Module: Authorization
 * =============================================================================
 *
 * Title: Introduction to Authorization
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Get a basic understanding how authorization is organized in Oak and become
 * familiar with distiction between access control management and permission
 * evaluation.
 *
 * Exercises:
 *
 * - Read JCR Specification and the Oak Documentation with focus on authorization
 *   and the distiction between access control management and permission
 *   evaluation.
 *
 *   Question: Can you explain the difference between access control management and permission evaluation?
 *
 * - Overview
 *   Take a look at the package structure in {@code org.apache.jackrabbit.oak.spi.security.authorization}
 *   and {@code org.apache.jackrabbit.oak.security.authorization} and try to
 *   become familiar with the interfaces and classes defined therein.
 *   Look for usages of the key entry points ({@link AccessControlManager} and
 *   {@link PermissionProvider} and try to get a big picture.
 *
 *   Question: Can you identify this distinction when looking at {@link AuthorizationConfiguration}?
 *   Question: What can you say about the usage of {@link AccessControlManager} in oak-core and oak-jcr?
 *   Question: What can you say about the usage of {@link PermissionProvider} in oak-core and oak-jcr?
 *
 * - Configuration
 *   Look at the default implementation of the {@link AuthorizationConfiguration}
 *   and try to identify the configurable parts. Compare your results with the
 *   Oak documentation.
 *
 *   Question: Can you provide a separate list for access control management and permission options?
 *   Question: Are the configuration options that affect both parts?
 *
 * - Pluggability
 *   Starting from the {@link AuthorizationConfiguration} again, investigate
 *   how the default implementation could be replaced.
 *
 *   Question: Is it possible to combine different authorization implementations?
 *
 * </pre>
 */
public class IntroductionTest extends AbstractSecurityTest {
}