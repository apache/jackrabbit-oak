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
package org.apache.jackrabbit.oak.query.ast;

import static com.google.common.collect.ImmutableSet.of;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Set;

import org.junit.Test;

public class OrImplTest {
    @Test
    public void simplifyForUnion() {
        ConstraintImpl op1, op2, op3, op4, or;
        Set<ConstraintImpl> expected;
        
        op1 = mock(ComparisonImpl.class);
        op2 = mock(ComparisonImpl.class);
        or = new OrImpl(op1, op2);
        expected = of(op1, op2);
        assertThat(or.convertToUnion(), is(expected));
        
        op1 = mock(ComparisonImpl.class);
        op2 = mock(ComparisonImpl.class);
        op3 = mock(ComparisonImpl.class);
        or = new OrImpl(new OrImpl(op1, op2), op3);
        expected = of(op1, op2, op3);
        assertThat(or.convertToUnion(), is(expected));

        op1 = mock(ComparisonImpl.class);
        op2 = mock(ComparisonImpl.class);
        op3 = mock(ComparisonImpl.class);
        op4 = mock(ComparisonImpl.class);
        or = new OrImpl(new OrImpl(new OrImpl(op1, op4), op2), op3);
        expected = of(op1, op2, op3, op4);
        assertThat(or.convertToUnion(), is(expected));
    }
}
