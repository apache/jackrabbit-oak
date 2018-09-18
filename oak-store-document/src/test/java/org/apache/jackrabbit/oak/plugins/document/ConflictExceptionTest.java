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
package org.apache.jackrabbit.oak.plugins.document;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

public class ConflictExceptionTest {

    @Test
    public void type() {
        ConflictException e = new ConflictException("conflict");
        CommitFailedException cfe = e.asCommitFailedException();
        assertEquals(CommitFailedException.MERGE, cfe.getType());
    }

    @Test
    public void cause() {
        ConflictException e = new ConflictException("conflict");
        CommitFailedException cfe = e.asCommitFailedException();
        assertSame(e, cfe.getCause());
    }

    @Test
    public void asCommitFailedException() {
        Revision r = Revision.newRevision(1);
        ConflictException e = new ConflictException("conflict", r);
        CommitFailedException cfe = e.asCommitFailedException();
        assertTrue(cfe instanceof FailedWithConflictException);
        FailedWithConflictException fwce = (FailedWithConflictException) cfe;
        assertEquals(CommitFailedException.MERGE, fwce.getType());
        assertEquals(Collections.singleton(r), fwce.getConflictRevisions());
    }
}
