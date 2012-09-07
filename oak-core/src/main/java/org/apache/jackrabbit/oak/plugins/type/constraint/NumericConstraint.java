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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import com.google.common.base.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class NumericConstraint<T> implements Predicate<Value> {
    private static final Logger log = LoggerFactory.getLogger(NumericConstraint.class);

    private boolean lowerInclusive;
    protected T lowerBound;
    protected T upperBound;
    private boolean upperInclusive;

    protected NumericConstraint(String definition) {
        // format: '(<min>, <max>)',  '[<min>, <max>]', '(, <max>)' etc.
        Pattern pattern = Pattern.compile("([\\(\\[])[^,]*,[^\\)\\]]*([\\)\\]])");
        Matcher matcher = pattern.matcher(definition);
        if (matcher.matches()) {
            // group 1 is lower inclusive/exclusive
            String match = matcher.group(1);
            lowerInclusive = "[".equals(match);

            // group 2 is lower, group 3 is upper  bound
            setBounds(matcher.group(2), matcher.group(3));

            // group 4 is lower inclusive/exclusive
            match = matcher.group(4);
            upperInclusive = "]".equals(match);
        }
        else {
            String msg = '\'' + definition + "' is not a valid value constraint format for numeric values";
            log.warn(msg);
        }
    }

    protected abstract void setBounds(String lowerBound, String upperBound);

    @Override
    public boolean apply(@Nullable Value value) {
        if (value == null) {
            return false;
        }

        try {
            T t = getValue(value);
            if (lowerBound != null) {
                if (lowerInclusive) {
                    if (less(t, lowerBound)) {
                        return false;
                    }
                } else {
                    if (lessOrEqual(t, lowerBound)) {
                        return false;
                    }
                }
            }
            if (upperBound != null) {
                if (upperInclusive) {
                    if (greater(t, upperBound)) {
                        return false;
                    }
                } else {
                    if (greaterOrEqual(t, upperBound)) {
                        return false;
                    }
                }
            }
            return true;
        }
        catch (RepositoryException e) {
            log.warn("Error checking numeric constraint " + this, e);
            return false;
        }
    }

    protected abstract T getValue(Value value) throws RepositoryException;
    protected abstract boolean less(T val, T bound);

    protected boolean greater(T val, T bound) {
        return less(bound, val);
    }

    protected boolean equals(T val, T bound) {
        return val.equals(bound);
    }

    protected boolean greaterOrEqual(T val, T bound) {
        return greater(val, bound) || equals(val, bound);
    }

    protected boolean lessOrEqual(T val, T bound) {
        return less(val, bound) || equals(val, bound);
    }

    @Override
    public String toString() {
        return (lowerInclusive ? "[" : "(") +
                (lowerBound == null ? "" : lowerBound) + ", " +
                (upperBound == null ? "" : upperBound) +
                (upperInclusive ? "]" : ")");
    }}
