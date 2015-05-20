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
package org.apache.jackrabbit.oak.security.authorization.accesscontrol;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <pre>
 * Module: Authorization (Access Control Management)
 * =============================================================================
 *
 * Title: AccessControlList Implementation Details
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand some of the implementation details applied by the default
 * access control list provided by the Oak access control management.
 *
 * Exercises:
 *
 * - {@link #TODO}
 *
 *
 * Additional Exercises:
 * -----------------------------------------------------------------------------
 *
 * The JCR specification mandates the that the principal used to create an ACE
 * is known to the system.
 *
 * - Investigate how the Oak repository can be configured such that creating
 *   ACEs with unknown principals would still succeed.
 *
 *   Question: Can you name the configuration option and list the allowed values? What are the differences?
 *   Question: Can you find other places in the access control management code
 *             base where this is being used?
 *   Question: Can you imagine the use cases for such a different or relaxed behaviour?
 *
 * </pre>
 */
public class AccessControlListImplTest extends AbstractSecurityTest {


}