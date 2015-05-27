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
import org.junit.Test;

/**
 * <pre>
 * Module: Authorization (Permission Evaluation)
 * =============================================================================
 *
 * Title: Representation of Permissions in the Repository
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand how the default implementation represents permissions in the repository.
 *
 * Exercises:
 *
 * - Overview
 *   Look {@code org/apache/jackrabbit/oak/plugins/nodetype/write/builtin_nodetypes.cnd}
 *   and try to identify the built in node types used to store permission
 *   content.
 *
 *   Question: Can explain the meaning of all types?
 *   Question: Why are most item definitions protected?
 *   Question: Can you identify node types that are not used? Can you explain why?
 *
 * - {@link #testReadOnly()}
 *   // TODO
 *
 *   Question: Can you explain why the permission store is read-only?
 *   Question: Can you identify the class(es) responsible for enforcing the read-only nature?
 *
 * - {@link #testAdministrativeAccessOnly()}
 *   // TODO
 *
 *   Question: Can you explain why the permission store is only accessible to administrative principals?
 *   Question: Can you identify the class(es) that actually enforce this?
 *
 * - {@link #TODO}
 *
 * </pre>
 */
public class L6_PermissionContentTest extends AbstractSecurityTest {

    @Test
    public void testReadOnly() {
        // TODO
    }

    @Test
    public void testAdministrativeAccessOnly() {
        // TODO
    }

}