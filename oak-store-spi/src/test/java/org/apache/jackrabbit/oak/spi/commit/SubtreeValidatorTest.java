/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.commit;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.junit.Test;

public class SubtreeValidatorTest {


    @Test
    public void testSubtreeValidator() throws CommitFailedException {
        Validator delegate = new FailingValidator();
        Validator validator = new SubtreeValidator(delegate, "one", "two");

        assertNull(validator.childNodeAdded("zero", EMPTY_NODE));
        assertNull(validator.childNodeChanged("two", EMPTY_NODE, EMPTY_NODE));
        assertNull(validator.childNodeDeleted("foo", EMPTY_NODE));

        assertNotNull(validator.childNodeAdded("one", EMPTY_NODE));
        assertNotNull(validator.childNodeChanged("one", EMPTY_NODE, EMPTY_NODE));
        assertNotNull(validator.childNodeDeleted("one", EMPTY_NODE));

        // Descend to the subtree
        validator = validator.childNodeChanged("one", EMPTY_NODE, EMPTY_NODE);
        assertNull(validator.childNodeAdded("zero", EMPTY_NODE));
        assertNull(validator.childNodeChanged("one", EMPTY_NODE, EMPTY_NODE));
        assertNull(validator.childNodeDeleted("foo", EMPTY_NODE));

        assertEquals(delegate, validator.childNodeAdded("two", EMPTY_NODE));
        assertEquals(delegate, validator.childNodeChanged("two", EMPTY_NODE, EMPTY_NODE));
        assertEquals(delegate, validator.childNodeDeleted("two", EMPTY_NODE));
    }

}
