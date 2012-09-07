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
package org.apache.jackrabbit.oak.plugins.type.constraint;

import javax.jcr.RepositoryException;
import javax.jcr.Value;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DoubleConstraint extends NumericConstraint<Double> {
    private static final Logger log = LoggerFactory.getLogger(DoubleConstraint.class);

    public DoubleConstraint(String constraint) {
        super(constraint);
    }

    @Override
    protected void setBounds(String lowerBound, String upperBound) {
        try {
            this.lowerBound = lowerBound == null || lowerBound.isEmpty()
                ? null
                : Double.parseDouble(lowerBound);

            this.upperBound = upperBound == null || upperBound.isEmpty()
                ? null
                : Double.parseDouble(upperBound);
        }
        catch (NumberFormatException e) {
            this.lowerBound = 1.0;
            this.upperBound = 0.0;
            log.warn("Invalid bound for numeric constraint" + this, e);
        }
    }

    @Override
    protected Double getValue(Value value) throws RepositoryException {
        return value.getDouble();
    }

    @Override
    protected boolean less(Double val, Double bound) {
        return val < bound;
    }
}
