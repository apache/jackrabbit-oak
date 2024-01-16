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
package org.apache.jackrabbit.oak.segment.remote;

import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class WriteAccessControllerTest {

    @Test
    public void testThreadBlocking() throws InterruptedException {
        WriteAccessController controller = new WriteAccessController();

        Thread t1 = new Thread(() -> {
            controller.checkWritingAllowed();
        });
        Thread t2 = new Thread(() -> {
            controller.checkWritingAllowed();
        });

        controller.disableWriting();

        t1.start();
        t2.start();

        Thread.sleep(200);

        assertThreadWaiting(t1.getState());
        assertThreadWaiting(t2.getState());

        controller.enableWriting();

        Thread.sleep(200);

        assertFalse(t1.isAlive());
        assertFalse(t2.isAlive());
    }

    private void assertThreadWaiting(Thread.State state) {
        assert state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING;
    }
}
