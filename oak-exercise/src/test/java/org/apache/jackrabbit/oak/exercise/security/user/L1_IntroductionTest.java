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
package org.apache.jackrabbit.oak.exercise.security.user;

import org.apache.jackrabbit.test.AbstractJCRTest;

/**
 * <pre>
 * Module: User Management
 * =============================================================================
 *
 * Title: Introduction to User Management
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand the usage of user management in Oak.
 *
 * Exercises:
 *
 * - Overview and Usages of User Management
 *   Search for usage of user management API (e.g. the {@link org.apache.jackrabbit.api.security.user.UserManager}
 *   interface in Oak. List your findings and discuss the impact.
 *
 *   Question: Where is the user management API being used?
 *   Question: What are the characteristics of this areas? E.g. are they configurable/pluggable?
 *   Question: What can you say about the usage of user management in the authorization code base?
 *
 * - Configuration
 *   Look at the default implementation of the {@link org.apache.jackrabbit.oak.spi.security.user.UserConfiguration}
 *   and try to identify the configurable parts. Compare your results with the
 *   Oak documentation.
 *
 *   Question: Can you provide a list of configuration options?
 *
 * - Pluggability
 *   Starting from the {@link UserConfiguration} again, investigate
 *   how the default implementation could be replaced.
 *
 *   Question: Is it possible to combine different user management implementations?
 *
 * </pre>
 */
public class L1_IntroductionTest extends AbstractJCRTest {
}