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
package org.apache.jackrabbit.oak.jcr.session;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import javax.jcr.Session;

import org.apache.jackrabbit.test.AbstractJCRTest;

public class SessionLogoutTest extends AbstractJCRTest {

    public void testConcurrentSessionLogout() throws Exception {
        Session s = getHelper().getReadWriteSession();
        List<Exception> exceptions = new CopyOnWriteArrayList<>();
        List<Thread> threads = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        for (int i = 0; i < 8; i++) {
            threads.add(new Thread(() -> {
                try {
                    latch.await();
                    s.logout();
                } catch (Exception e) {
                    exceptions.add(e);
                }
            }));
        }
        for (Thread t : threads) {
            t.start();
        }
        latch.countDown();
        for (Thread t : threads) {
            t.join();
        }
        for (Exception ex : exceptions) {
            ex.printStackTrace();
            fail(ex.toString());
        }
    }
}
