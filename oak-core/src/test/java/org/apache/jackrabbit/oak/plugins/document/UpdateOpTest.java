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

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for UpdateOp
 */
public class UpdateOpTest {

    @Test
    public void keyEquals() {
        Revision r1 = Revision.newRevision(1);
        Revision r2 = Revision.newRevision(1);

        UpdateOp.Key k1 = new UpdateOp.Key("foo", null);
        UpdateOp.Key k2 = new UpdateOp.Key("bar", null);
        assertFalse(k1.equals(k2));
        assertFalse(k2.equals(k1));

        UpdateOp.Key k3 = new UpdateOp.Key("foo", null);
        assertTrue(k1.equals(k3));
        assertTrue(k3.equals(k1));
        
        UpdateOp.Key k4 = new UpdateOp.Key("foo", r1);
        assertFalse(k4.equals(k3));
        assertFalse(k3.equals(k4));
        
        UpdateOp.Key k5 = new UpdateOp.Key("foo", r2);
        assertFalse(k5.equals(k4));
        assertFalse(k4.equals(k5));
        
        UpdateOp.Key k6 = new UpdateOp.Key("foo", r1);
        assertTrue(k6.equals(k4));
        assertTrue(k4.equals(k6));

        UpdateOp.Key k7 = new UpdateOp.Key("bar", r1);
        assertFalse(k7.equals(k6));
        assertFalse(k6.equals(k7));
    }

    
}
