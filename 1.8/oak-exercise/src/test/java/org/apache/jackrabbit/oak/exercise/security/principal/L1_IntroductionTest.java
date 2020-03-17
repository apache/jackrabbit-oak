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
package org.apache.jackrabbit.oak.exercise.security.principal;

import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.test.AbstractJCRTest;

/**
 * <pre>
 * Module: Principal Management
 * =============================================================================
 *
 * Title: Introduction to Principal Management
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand the usage of principal management in Oak and become familiar with
 * the difference between the Jackrabbit {@link PrincipalManager} and the
 * {@link PrincipalProvider} exposed by Oak SPI.
 *
 * Exercises:
 *
 * - Overview and Usages of Principal Management
 *   Search for usage of principal management API (e.g. the {@link org.apache.jackrabbit.api.security.principal.PrincipalManager}
 *   and {@link org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider}
 *   interface in Oak _and_ Jackrabbit JCR Commons. List your findings and discuss the impact.
 *
 *   Question: Where is the principal manager|provider being used for?
 *   Question: Who is the expected API consumer?
 *   Question: What are the characteristics of this areas?
 *   Question: What can you say about the relation of principal management and authentication?
 *   Question: What can you say about the relation of principal management and authorization?
 *
 *
 * - Configuration
 *   Look at the default implementation of the {@link org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration}
 *   and try to identify the configurable parts. Compare your results with the
 *   Oak documentation.
 *
 *   Question: Can you provide a list of configuration options?
 *
 * - Pluggability
 *   Become familar with the pluggable nature of the principal management
 *
 *   Question: What means does Oak provide to change or extend the set of principals exposed?
 *   Question: What interfaces do you need to implement?
 *   Question: Is it possible to combine different principal implementations? How does that work?
 *
 *
 * Additional Exercises:
 * -----------------------------------------------------------------------------
 *
 * - Discuss why principal management API is read only.
 *
 *   Question: How are principals exposed by the {@link PrincipalManager} collected?
 *   Question: How does the default implementation look like?
 *
 *
 * Advanced Exercises:
 * -----------------------------------------------------------------------------
 *
 * If you want to dig deeper into the principal management implementation details
 * you may want to play around with plugging your custom principal provider instance
 * or replacing the default setup altogether.
 *
 * - Write your your custom implemenation of the principal provider and deploy it
 *   in an OSGi based repository setup. Observe the effect it has on principal
 *   management, authentication and authorization.
 *
 *
 * Related Exercises
 * -----------------------------------------------------------------------------
 *
 * - {@link L2_PrincipalManagerTest}
 * - {@link L4_PrincipalProviderTest}
 * - {@link L3_EveryoneTest}
 *
 * </pre>
 *
 * @see org.apache.jackrabbit.api.security.principal.PrincipalManager
 * @see org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider
 */
public class L1_IntroductionTest extends AbstractJCRTest {

}