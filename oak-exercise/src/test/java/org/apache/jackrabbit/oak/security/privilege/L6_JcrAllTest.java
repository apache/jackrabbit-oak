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
package org.apache.jackrabbit.oak.security.privilege;

import javax.jcr.security.Privilege;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.junit.Test;

/**
 * <pre>
 * Module: Privilege Management
 * =============================================================================
 *
 * Title: The Privilege 'jcr:all'
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand the meaning of the jcr:all privilege and how it is maintained
 * in the Oak repository.
 *
 * Exercises:
 *
 * - Overview
 *   Read again what JSR 283 states about {@link Privilege#JCR_ALL) and review
 *   again the result of {@link L5_CustomPrivilegeTest#testJcrAll()}
 *
 * - {@link #testManualModification()}
 *   TODO
 *
 * Advanced Exercise
 * -----------------------------------------------------------------------------
 *
 * - {@link #testJcrAllInPermissionStore()}
 *   TODO
 *
 * </pre>
 */
public class L6_JcrAllTest extends AbstractSecurityTest {

    @Test
    public void testManualModification() {
        // TODO
    }

    @Test
    public void testJcrAllInPermissionStore() {
        // TODO
    }
}