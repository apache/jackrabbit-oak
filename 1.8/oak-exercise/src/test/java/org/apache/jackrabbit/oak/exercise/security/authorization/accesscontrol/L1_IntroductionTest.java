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
package org.apache.jackrabbit.oak.exercise.security.authorization.accesscontrol;

import org.apache.jackrabbit.oak.AbstractSecurityTest;

/**
 * <pre>
 * Module: Authorization (Access Control Management)
 * =============================================================================
 *
 * Title: Introduction to Access Control Management
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Become familiar with the JCR Access Control Management API and the extensions
 * provided by Jackrabbit API. Understand how access control management is
 * used and exposed in Oak and finally gain insight into some details of the
 * default implementation.
 *
 * Exercises:
 *
 * - Overview and Usages of AccessControl Management
 *   Search for usage of the access control management API (e.g. {@link javax.jcr.security.AccessControlManager})
 *   in Oak _and_ Jackrabbit JCR Commons.
 *
 *   Question: Where is the access control manager being used for?
 *   Question: Who is the expected API consumer?
 *   Question: What are the characteristics of this areas?
 *   Question: Can you identify areas where oak-jcr and oak-core actually make use of the access control management API?
 *
 * - Configuration
 *   Look at the default implementation(s) of the {@link org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration}
 *   and try to identify the configurable parts with respect to access control.
 *   Compare your results with the Oak documentation.
 *
 *   Question: Can you provide a list of configuration options for access control s.str.?
 *   Question: Can you identify where these configuration options are being evaluated?
 *   Question: Which options also affect the permission evaluation?
 *
 * - Pluggability
 *   Become familar with the pluggable parts of the access control management
 *
 *   Question: What means does Oak provide to change or extend the access control management?
 *   Question: Can you identify the interfaces that you needed to implement?
 *   Question: Would it be possible to only replace the implementation of {@link javax.jcr.security.AccessControlManager}?
 *             How could you achieve this?
 *             And what would be the consequences for the whole authorization module?
 *
 *
 * Advanced Exercises:
 * -----------------------------------------------------------------------------
 *
 * See {@link L7_RestrictionsTest}
 * for some advanced exercises with respect to custom restrictions.
 *
 *
 * Related Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link L2_AccessControlManagerTest}
 * - {@link L3_AccessControlListTest}
 * - {@link L4_EffectivePoliciesTest}
 * - {@link L7_RestrictionsTest}
 *
 * </pre>
 */
public class L1_IntroductionTest extends AbstractSecurityTest {


}