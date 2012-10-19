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
package org.apache.jackrabbit.oak.security.user;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractOakTest;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtility;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * UserManagerImplTest...
 */
public class UserManagerImplTest extends AbstractOakTest {

     @Override
    protected ContentRepository createRepository() {
        // TODO
        return null;
    }

//    @Test
//    public void testSetPassword() throws Exception {
//        UserManagerImpl userMgr = createUserManager();
//        User user = userMgr.createUser("a", "pw");
//
//        List<String> pwds = new ArrayList<String>();
//        pwds.add("pw");
//        pwds.add("");
//        pwds.add("{sha1}pw");
//
//        for (String pw : pwds) {
//            user.setPassword(user, pw, true);
//            String pwHash = up.getPasswordHash(user);
//            assertNotNull(pwHash);
//            assertTrue(PasswordUtility.isSame(pwHash, pw));
//        }
//
//        for (String pw : pwds) {
//            up.setPassword(user, pw, false);
//            String pwHash = up.getPasswordHash(user);
//            assertNotNull(pwHash);
//            if (!pw.startsWith("{")) {
//                assertTrue(PasswordUtility.isSame(pwHash, pw));
//            } else {
//                assertFalse(PasswordUtility.isSame(pwHash, pw));
//                assertEquals(pw, pwHash);
//            }
//        }
//    }
//
//    @Test
//    public void setPasswordNull() throws Exception {
//        UserProviderImpl up = createUserProvider();
//        Tree user = up.createUser("a", null);
//
//        try {
//            up.setPassword(user, null, true);
//            fail("setting null password should fail");
//        } catch (IllegalArgumentException e) {
//            // expected
//        }
//
//        try {
//            up.setPassword(user, null, false);
//            fail("setting null password should fail");
//        } catch (IllegalArgumentException e) {
//            // expected
//        }
//    }


//
//    @Test
//    public void testGetPasswordHash() throws Exception {
//        UserProviderImpl up = createUserProvider();
//        Tree user = up.createUser("a", null);
//
//        assertNull(up.getPasswordHash(user));
//    }

}