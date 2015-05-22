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
package org.apache.jackrabbit.oak.security.authorization.permission;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <pre>
 * Module: Authorization (Permission Evaluation)
 * =============================================================================
 *
 * Title: Introduction
 * -----------------------------------------------------------------------------
 *
 * Become familiar with the way to verify permissions using JCR API.
 * Get a basic understanding how permission evaluation is used and exposed in Oak
 * and finally gain insight into some details of the default implementation.
 *
 * Exercises:
 *
 * - Overview and Usages of Permission Evaluation
 *   Search and list for permission lated methods in the JCR API and recap what
 *   the specification states about permissions compared to access control
 *   management.
 *
 *   Question: What are the areas in JCR that deal with permissions?
 *   Question: Who is the expected API consumer?
 *   Question: Can you elaborate when it actually makes sense to use this API?
 *   Question: Can you think about potential drawbacks of doing so?
 *
 * - Permission Evaluation in Oak
 *   In a second step try to become more familiar with the nature of the
 *   permission evaluation in Oak.
 *
 *   Question: What is the nature of the public SPI?
 *   Question: Can you identify the main entry point for permission evaluation?
 *   Question: Can you identify to exact location(s) in Oak where read-access is being enforced?
 *   Question: Can you identify the exact location(s) in Oak where all kind of write access is being enforced?
 *
 * - Configuration
 *   Look at the default implementation(s) of the {@link org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration}
 *   and try to identify the configurable parts with respect to permission evaluation.
 *   Compare your results with the Oak documentation.
 *
 *   Question: Can you provide a list of configuration options for the permission evaluation?
 *   Question: Can you identify where these configuration options are being evaluated?
 *   Question: Which options also affect the access control management?
 *
 * - Pluggability
 *   Become familar with the pluggable parts of the permission evaluation
 *
 *   Question: What means does Oak provide to change or extend the permission evaluation?
 *   Question: Can you identify the interfaces that you needed to implement?
 *   Question: Would it be possible to only replace the implementation of {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider}?
 *             How could you achieve this?
 *             And what would be the consequences for the whole authorization module?
 *
 *
 * Related Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link org.apache.jackrabbit.oak.security.authorization.permission.L2_PermissionDiscoveryTest}
 *
 * </pre>
 */
public class L1_IntroductionTest extends AbstractSecurityTest {

}