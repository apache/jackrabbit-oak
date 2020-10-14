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
package org.apache.jackrabbit.oak.benchmark.authorization;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.security.AccessControlManager;

/**
 * Test effect of PermissionProvider.refresh called upon Session.save(). In contrast to {@link RefreshHasItemGetItemTest}
 * a separate Session is created for the save operation to reflect the scenario where the session is logged-out after
 * saving changes.
 */
public class SaveHasItemGetItemTest extends AbstractHasItemGetItemTest {

    private static final Logger log = LoggerFactory.getLogger(SaveHasItemGetItemTest.class);

    public SaveHasItemGetItemTest(int itemsToRead, int numberOfACEs, int numberOfGroups, boolean doReport) {
        super(itemsToRead, numberOfACEs, numberOfGroups, doReport);
    }

    @NotNull
    @Override
    String additionalMethodName() {
        return "save";
    }

    @Override
    void additionalOperations(@NotNull String path, @NotNull Session s, @NotNull AccessControlManager acMgr) {
        Session otherS = null;
        try {
            otherS = getTestSession();
            otherS.save();
        } catch (RepositoryException e) {
            if (doReport) {
                e.printStackTrace(System.out);
            }
        } finally {
            if (otherS != null && otherS.isLive()) {
                otherS.logout();
            }
        }
    }
}