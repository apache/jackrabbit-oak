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
package org.apache.jackrabbit.oak.jcr;

import static org.junit.Assert.fail;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.ValueFactory;
import javax.jcr.ValueFormatException;

import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.junit.Before;
import org.junit.Test;

/**
 * ValueFactoryTest...
 */
public class ValueFactoryTest extends AbstractRepositoryTest {

    private ValueFactory valueFactory;

    public ValueFactoryTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setup() throws RepositoryException {
        valueFactory = getAdminSession().getValueFactory();
    }

    // TODO: this tests should be moved to the TCK. retrieving "invalidIdentifier" from the config.

    @Test
    public void testReferenceValue() {
        try {
            valueFactory.createValue("invalidIdentifier", PropertyType.REFERENCE);
            fail("Conversion to REFERENCE value must validate identifier string ");
        } catch (ValueFormatException e) {
            // success
        }
    }

    @Test
    public void testWeakReferenceValue() {
        try {
            valueFactory.createValue("invalidIdentifier", PropertyType.WEAKREFERENCE);
            fail("Conversion to WEAK_REFERENCE value must validate identifier string ");
        } catch (ValueFormatException e) {
            // success
        }
    }

}
