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
package org.apache.jackrabbit.oak.plugins.nodetype.constraint;

import java.util.Calendar;

import javax.jcr.RepositoryException;
import javax.jcr.Value;

import org.apache.jackrabbit.value.DateValue;

public class DateConstraint extends NumericConstraint<Calendar> {
    public DateConstraint(String definition) {
        super(definition);
    }

    @Override
    protected Calendar getBound(String bound) {
        try {
            return DateValue.valueOf(bound).getDate();
        }
        catch (RepositoryException e) {
            throw (NumberFormatException) new NumberFormatException().initCause(e);
        }
    }

    @Override
    protected Calendar getValue(Value value) throws RepositoryException {
        return value.getDate();
    }

    @Override
    protected boolean less(Calendar val, Calendar bound) {
        return val.getTimeInMillis() < bound.getTimeInMillis();
    }

    @Override
    protected boolean equals(Calendar val, Calendar bound) {
        return val.getTimeInMillis() == bound.getTimeInMillis();
    }
}
