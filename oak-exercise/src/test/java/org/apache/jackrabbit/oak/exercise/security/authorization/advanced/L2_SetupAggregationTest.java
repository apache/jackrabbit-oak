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
package org.apache.jackrabbit.oak.exercise.security.authorization.advanced;

/**
 * <pre>
 * Module: Advanced Authorization Topics
 * =============================================================================
 *
 * Title: Aggregating Multiple Authorization Models : Setup
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Learn how to deploy authorization models and setup Oak authorization with more
 * than one models.
 *
 * Note, that this section only focuses on OSGi-based Oak setup scenarios.
 *
 * Exercises:
 *
 * - Deploy Bundle
 *   Take a bundle that provides you with another implementation of AuthorizationConfiguration
 *   and deploy it with your OSGi based Oak setup.
 *
 *   Hint: Oak comes with 2 additional authorization models, which you can use
 *   > Closed User Groups in oak-authorization-cug (see also http://jackrabbit.apache.org/oak/docs/security/authorization/cug.html)
 *   > Read Only in oak-exercise
 *
 *   Questions:
 *   > Can you identify the OSGi components that come with the model?
 *   > Does your model require any mandatory configuration in order to be functional?
 *
 * - Adjust Configuration of 'Apache Jackrabbit Oak SecurityProvider'
 *   In a second step you should adjust the configuration of the SecurityProvider
 *   in order to make sure the additional AuthorizationConfiguration is properly
 *   wired with the security setup.
 *
 *   > Add the addition configuration to the list of required service IDs (see also http://jackrabbit.apache.org/oak/docs/security/introduction.html)
 *   > Check the value of 'Authorization Composition Type'.
 *   > Observe the log INFOs to verify the SecurityProvider is properly registered
 *   > Inspect the references to 'authorizationConfiguration' in org.apache.jackrabbit.oak.security.internal.SecurityProviderRegistration
 *     and verify that the extra module shows up there.
 *
 * - Verify Access Control Management and Permission Evaluation
 *   Before moving on think about your expectations wrt result of the aggregation
 *   both in terms of access control management and permission evaluation.
 *
 *
 * Advanced Exercises:
 * -----------------------------------------------------------------------------
 *
 * - Play with the configuration option 'Authorization Composition Type'.
 *
 *   Questions:
 *   > Would it be an option to use "OR" as the composition type with the setup you chose?
 *   > What would be the result if this was a valid option?
 *   > If it was a valid option, imaging a setup scenario where it would not work.
 *   > If it wasn't an option, explain why. Think about a scenario where it was valid.
 *
 * </pre>
 */
public class L2_SetupAggregationTest {


}