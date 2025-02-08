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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Session;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import java.security.Principal;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MoveRenameTest extends AbstractCugTest {

    private static final Logger log = LoggerFactory.getLogger(MoveRenameTest.class);

    private Principal testgroup;
    private Principal everyone;

    // cug at "/content/a" (below SUPPORTED_PATH)
    private String testPath;
    // nested cug at "/content/a/b/c" (below 'testPath')
    private String nestedPath;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        testgroup = getTestGroupPrincipal();
        everyone = EveryonePrincipal.getInstance();

        // cug at "/content/a" for test-group (below SUPPORTED_PATH)
        // nested cug at "/content/a/b/c" for everyone (below 'testPath')
        // cug at "/content2" for everyone (i.e. directly at SUPPORTED_PATH2), no nested CUGs
        setupCugsAndAcls("/content/a", "/content/a/b/c", "/content2");
        testPath = PathUtils.concat(SUPPORTED_PATH ,new String[] {"a"});
        nestedPath = PathUtils.concat(SUPPORTED_PATH ,"a", "b", "c");

        assertCugPrivileges(testgroup, true, testPath);
        assertCugPrivileges(everyone, true, nestedPath, SUPPORTED_PATH2);

        assertCugPrivileges(testgroup, false, nestedPath, SUPPORTED_PATH2);
        assertCugPrivileges(everyone, false, testPath);
    }

    private void assertCugPrivileges(@NotNull Principal principal, boolean isGranted, String... paths) {
        CugPermissionProvider pp = createCugPermissionProvider(Set.of(SUPPORTED_PATHS), principal);
        for (String path : paths) {
            assertEquals(isGranted, pp.isGranted(path, Session.ACTION_READ));
        }
    }

    private void removeCug(@NotNull String path) throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        for (AccessControlPolicy policy : acMgr.getPolicies(path)) {
            if (policy instanceof CugPolicyImpl) {
                acMgr.removePolicy(path, policy);
                root.commit();
                return;
            }
        }
        fail("No CUG policy to remove at " + path);
    }

    @Test
    public void testMove() throws Exception {
        String destPath = SUPPORTED_PATH3 + "/content";
        root.move(SUPPORTED_PATH2, destPath);
        root.commit();

        assertCugPrivileges(everyone, true, destPath);
        assertCugPrivileges(everyone, false, SUPPORTED_PATH2);

        assertNestedCugs(root, getRootProvider(), PathUtils.ROOT_PATH, false, destPath,  testPath);
    }

    @Test
    public void testMoveAndRename() throws Exception {
        // move
        String destPath = SUPPORTED_PATH3 + "/content";
        root.move(SUPPORTED_PATH2, destPath);
        // rename with same transient operation
        String destPath2 = SUPPORTED_PATH3 + "/content2";
        root.move(destPath, destPath2);
        root.commit();

        assertCugPrivileges(everyone, true, destPath2);
        assertCugPrivileges(everyone, false, destPath);
        assertCugPrivileges(everyone, false, SUPPORTED_PATH2);

        assertNestedCugs(root, getRootProvider(), PathUtils.ROOT_PATH, false, destPath2,  testPath);
    }

    @Test
    public void testMoveAndBack() throws Exception {
        String destPath = SUPPORTED_PATH3 + "/content";
        root.move(SUPPORTED_PATH2, destPath);
        root.commit();

        // move back to original location after comitting
        root.move(destPath, SUPPORTED_PATH2);
        root.commit();

        assertCugPrivileges(everyone, true, SUPPORTED_PATH2);
        assertCugPrivileges(everyone, false, destPath);

        assertNestedCugs(root, getRootProvider(), PathUtils.ROOT_PATH, false, SUPPORTED_PATH2,  testPath);
    }

    @Test
    public void testMoveWithNested() throws Exception {
        String destPath = SUPPORTED_PATH3 + "/content";
        root.move(testPath, destPath);
        root.commit();

        String nestedDestPath = PathUtils.concat(destPath, PathUtils.relativize(testPath, nestedPath));
        assertCugPrivileges(testgroup, true, destPath);
        assertCugPrivileges(everyone, true, nestedDestPath);
        assertCugPrivileges(testgroup, false, nestedDestPath);

        assertCugPrivileges(testgroup, false, testPath);
        assertCugPrivileges(everyone, false, nestedPath);

        assertNestedCugs(root, getRootProvider(), destPath, true, nestedDestPath);
    }

    @Test
    public void testMoveWithNestedAndRename() throws Exception {
        // move
        String destPath = SUPPORTED_PATH3 + "/content";
        root.move(testPath, destPath);
        // rename
        String destPath2 = SUPPORTED_PATH3 + "/content2";
        root.move(destPath, destPath2);
        root.commit();

        assertCugPrivileges(testgroup, true, destPath2);
        assertCugPrivileges(testgroup, false, destPath);

        String nestedDestPath = PathUtils.concat(destPath2, PathUtils.relativize(testPath, nestedPath));
        assertCugPrivileges(everyone, true, nestedDestPath);
        assertCugPrivileges(testgroup, false, nestedDestPath);

        assertNestedCugs(root, getRootProvider(), destPath2, true, nestedDestPath);
    }

    @Test
    public void testMoveParentWithNested() throws Exception {
        String destPath = SUPPORTED_PATH2 + "/content";
        root.move(SUPPORTED_PATH, destPath);
        root.commit();

        String destCugPath = PathUtils.concat(destPath, PathUtils.relativize(SUPPORTED_PATH, testPath));
        assertNestedCugs(root, getRootProvider(), PathUtils.ROOT_PATH, false, SUPPORTED_PATH2);
        assertNestedCugs(root, getRootProvider(), SUPPORTED_PATH2, true, destCugPath);
        String destNestedCugPath = PathUtils.concat(destCugPath, PathUtils.relativize(testPath, nestedPath));
        assertNestedCugs(root, getRootProvider(), destCugPath, true, destNestedCugPath);
    }

    @Test
    public void testMoveParentWithNestedAndBack() throws Exception {
        String destPath = SUPPORTED_PATH3 + "/content";
        root.move(SUPPORTED_PATH, destPath);
        root.commit();

        // move back to original location
        root.move(destPath, SUPPORTED_PATH);
        root.commit();

        assertCugPrivileges(testgroup, true, testPath);
        assertCugPrivileges(everyone, true, nestedPath);
        assertCugPrivileges(testgroup, false, nestedPath);

        assertNestedCugs(root, getRootProvider(), PathUtils.ROOT_PATH, false, testPath,  SUPPORTED_PATH2);
        assertNestedCugs(root, getRootProvider(), testPath, true, nestedPath);
    }

    @Test
    public void testRemoveCugAfterMoveParentWithNested() throws Exception {
        String destPath = SUPPORTED_PATH3 + "/content";
        root.move(SUPPORTED_PATH, destPath);
        root.commit();

        String cugDestPath = PathUtils.concat(destPath, "a");
        String nestedCugDestPath = PathUtils.concat(cugDestPath, PathUtils.relativize(testPath, nestedPath));
        assertCugPrivileges(testgroup, false, destPath);
        assertCugPrivileges(everyone, true, nestedCugDestPath);
        assertCugPrivileges(testgroup, false, nestedCugDestPath);

        assertNestedCugs(root, getRootProvider(), PathUtils.ROOT_PATH, false, cugDestPath,  SUPPORTED_PATH2);
        assertNestedCugs(root, getRootProvider(), cugDestPath, true, nestedCugDestPath);

        removeCug(cugDestPath);

        assertNestedCugs(root, getRootProvider(), PathUtils.ROOT_PATH, false, nestedCugDestPath,  SUPPORTED_PATH2);
    }

    @Test
    public void testRenameWithNested() throws Exception {
        String destPath = "/content/a2";
        root.move(testPath, destPath);
        root.commit();

        assertCugPrivileges(testgroup, true, destPath);
        assertCugPrivileges(everyone, true, PathUtils.concat(destPath, PathUtils.relativize(testPath, nestedPath)));

        assertNestedCugs(root, getRootProvider(), PathUtils.ROOT_PATH, false, destPath,  SUPPORTED_PATH2);
        assertNestedCugs(root, getRootProvider(), destPath, true, destPath + "/b/c");
    }

    @Test
    public void testRemoveCugAfterRenameWithNested() throws Exception {
        String destPath = "/content/a2";
        root.move(testPath, destPath);
        root.commit();

        removeCug(destPath);

        String nestedDepthPath = PathUtils.concat(destPath, PathUtils.relativize(testPath, nestedPath));
        assertCugPrivileges(testgroup, false, testPath);
        assertCugPrivileges(testgroup, false, destPath);
        assertCugPrivileges(everyone, true, nestedDepthPath);

        assertNestedCugs(root, getRootProvider(), PathUtils.ROOT_PATH, false, nestedDepthPath,  SUPPORTED_PATH2);
    }

    @Test
    public void testRemoveCugAfterDoubleRenameWithNested() throws Exception {
        root.move(testPath, "/content/a2");
        root.commit();

        root.move("/content/a2", testPath);
        root.commit();

        assertNestedCugs(root, getRootProvider(), PathUtils.ROOT_PATH, false, testPath,  SUPPORTED_PATH2);
        assertNestedCugs(root, getRootProvider(), testPath, true, nestedPath);

        removeCug(testPath);

        assertCugPrivileges(testgroup, false, testPath);
        assertCugPrivileges(everyone, true, nestedPath);

        assertNestedCugs(root, getRootProvider(), PathUtils.ROOT_PATH, false, nestedPath,  SUPPORTED_PATH2);
    }
}