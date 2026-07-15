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
package org.apache.jackrabbit.oak.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.jcr.Session;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;

public class GetAuthorizableByIdTest extends AbstractTest {

    private final int numberOfUsers;
    private final boolean flat;

    private final List<String> ids = new ArrayList<String>();

    public GetAuthorizableByIdTest(int numberOfUsers, boolean flatStructure) {
        this.numberOfUsers = numberOfUsers;
        this.flat = flatStructure;
    }

    @Override
    protected void beforeSuite() throws Exception {
        Session s = loginWriter();
        UserManager userManager = ((JackrabbitSession) s).getUserManager();
        for (int i = 0; i < numberOfUsers; i++) {
            String id = (flat) ? "user" + i : UUID.randomUUID().toString();
            User user = userManager.createUser(id, id);
            ids.add(id);
        }
        s.save();
        System.out.println("Setup "+numberOfUsers+" users");
    }

    @Override
    protected void afterSuite() throws Exception {
        Session session = loginWriter();
        session.getNode(UserConstants.DEFAULT_USER_PATH + "/u").remove();
        session.save();
    }

    @Override
    protected void runTest() throws Exception {
        Session s = loginWriter();
        UserManager userManager = ((JackrabbitSession) s).getUserManager();
        for (int i = 0; i < 1000; i++) {
            Authorizable a = userManager.getAuthorizable(getUserId());
        }

    }

    protected String getUserId() {
        return ids.get((int) Math.floor(numberOfUsers * Math.random()));
    }
}