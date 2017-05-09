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
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;
import javax.jcr.Session;

import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.junit.Assert;

/**
 * Test the performance of adding a configured number of members to groups. The
 * following parameters can be used to run the benchmark:
 *
 * - numberOfMembers : the number of members that should be added in the test run
 * - batchSize : the size of the memberID-array to be passed to the addMembers call
 *
 * In contrast to {@link AddMembersTest}, this benchmark will always add unique
 * members to the given group.
 */
public class AddUniqueMembersTest extends AddMembersTest {

    private static AtomicLong index = new AtomicLong();

    public AddUniqueMembersTest(int numberOfMembers, int batchSize) {
        super(numberOfMembers, batchSize, ImportBehavior.NAME_BESTEFFORT);
    }

    @Override
    protected void createUsers(@Nonnull UserManager userManager) throws Exception {
        // no need for creating the users beforehand
    }

    @Override
    protected void addMembers(@Nonnull UserManager userManger, @Nonnull Group group, @Nonnull Session s)
            throws Exception {
        long uid = index.getAndIncrement();

        for (int i = 0; i <= numberOfMembers; i++) {
            Set<String> failed;
            if (batchSize <= DEFAULT_BATCH_SIZE) {
                failed = group.addMembers(USER + i + "_" + uid);
            } else {
                List<String> ids = new ArrayList<String>(batchSize);
                for (int j = 0; j < batchSize && i <= numberOfMembers; j++) {
                    ids.add(USER + i + "_" + uid);
                    i++;
                }
                failed = group.addMembers(ids.toArray(new String[ids.size()]));
            }
            Assert.assertTrue("Group " + group.getID() + ": unable to add: " + failed, failed.isEmpty());
            s.save();
        }
    }
}
