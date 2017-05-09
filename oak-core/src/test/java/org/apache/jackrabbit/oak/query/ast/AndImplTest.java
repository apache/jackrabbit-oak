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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableSet.of;
import static java.util.Collections.emptySet;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Set;

import javax.annotation.Nonnull;

import org.junit.Test;

public class AndImplTest {
    @Test
    public void simplifyForUnion() {
        ConstraintImpl and, op1, op2, op3, op4;
        Set<ConstraintImpl> expected;
        
        op1 = mock(ComparisonImpl.class);
        op2 = mock(ComparisonImpl.class);
        and = new AndImpl(op1, op2);
        expected = emptySet();
        assertThat(and.convertToUnion(), is(expected));

        op1 = mockConstraint("op1", ComparisonImpl.class);
        op2 = mockConstraint("op2", ComparisonImpl.class);
        op3 = mockConstraint("op3", ComparisonImpl.class);
        and = new AndImpl(new OrImpl(op1, op2), op3);
        expected = of(
            (ConstraintImpl) new AndImpl(op1, op3)
            , (ConstraintImpl) new AndImpl(op2, op3)
        );
        assertThat(and.convertToUnion(), is(expected));

        op1 = mockConstraint("op1", ComparisonImpl.class);
        op2 = mockConstraint("op2", ComparisonImpl.class);
        op3 = mockConstraint("op3", ComparisonImpl.class);
        op4 = mockConstraint("op4", ComparisonImpl.class);
        and = new AndImpl(new OrImpl(new OrImpl(op1, op4), op2), op3);
        expected = of(
            (ConstraintImpl) new AndImpl(op1, op3)
            , (ConstraintImpl) new AndImpl(op2, op3)
            , (ConstraintImpl) new AndImpl(op4, op3)
        );
        assertThat(and.convertToUnion(), is(expected));
}
    
    /**
     * convenience method for having better assertion messages 
     * 
     * @param toString the {@link String#toString()} message to be shown. Cannot be null;
     * @param clazz the class you want Mockito to generate for you.
     * @return a Mockito instance of the provided ConstraintImpl
     */
    private static ConstraintImpl mockConstraint(@Nonnull String toString, 
                                                 @Nonnull Class<? extends ConstraintImpl> clazz) {
        ConstraintImpl c = mock(checkNotNull(clazz));
        when(c.toString()).thenReturn(checkNotNull(toString));
        return c;
    }
}
