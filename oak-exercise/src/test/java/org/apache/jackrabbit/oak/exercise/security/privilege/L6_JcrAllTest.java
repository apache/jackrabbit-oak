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
package org.apache.jackrabbit.oak.exercise.security.privilege;

import javax.jcr.security.Privilege;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeUtil;
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
 *   again the result of {@link L4_CustomPrivilegeTest#testJcrAll()}
 *
 * - {@link #testManualModification()}
 *   This test case tries to modify the tree storing the jcr:all privilege
 *   definition. Walk through the test and explain what happens.
 *   Fix the test case such that it passes.
 *
 *   Question: Can you identify the relevant class in the Oak code base?
 *   Question: Can you explain what it does and why?
 *
 *
 * Advanced Exercise
 * -----------------------------------------------------------------------------
 *
 * Mapping jcr:all in the permission store:
 *
 * - Due to the dynamic nature of jcr:all the long-representation of this privilege
 *   in the permission store may change over time. This exercise aims to illustrate
 *   how granting|denying jcr:all is reflected in the permission store.
 *
 *   In your preferred repository browser:
 *   Create multiple access control entries for different principals including
 *   on granting/denying jcr:all at a given existing path. Identify the corresponding
 *   entries in the permission store and describe the nature of the 'rep:privileges'
 *   properties.
 *
 *   Question: How is jcr:all represented?
 *   Question: What is the difference compared to entries granting/denying other privileges?
 *
 *   Discuss your findings and explain the special behavior for jcr:all.
 *
 * </pre>
 */
public class L6_JcrAllTest extends AbstractSecurityTest {

    @Test
    public void testManualModification() throws Exception {
        // EXERCISE: fix the test case such that it passes.

        Tree jcrAllTree = PrivilegeUtil.getPrivilegesTree(root).getChild(PrivilegeConstants.JCR_ALL);

        jcrAllTree.removeProperty(PrivilegeConstants.REP_AGGREGATES);
        root.commit();
    }
}