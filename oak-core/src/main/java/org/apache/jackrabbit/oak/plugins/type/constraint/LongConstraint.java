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

public class LongConstraint implements Predicate<Value> {
    private static final Logger log = LoggerFactory.getLogger(LongConstraint.class);

    private boolean lowerInclusive;
    private Long lowerBound;
    private Long upperBound;
    private boolean upperInclusive;

    public LongConstraint(String definition) {
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

    private void setBounds(String lowerBound, String upperBound) {
        try {
            this.lowerBound = lowerBound == null || lowerBound.isEmpty()
                ? null
                : Long.parseLong(lowerBound);

            this.upperBound = upperBound == null || upperBound.isEmpty()
                ? null
                : Long.parseLong(upperBound);
        }
        catch (NumberFormatException e) {
            this.lowerBound = 1L;
            this.upperBound = 0L;
            log.warn("Invalid bound for numeric constraint" + this, e);
        }
    }

    @Override
    public boolean apply(@Nullable Value value) {
        if (value == null) {
            return false;
        }

        try {
            long val = value.getLong();
            if (lowerBound != null) {
                if (lowerInclusive) {
                    if (val < (lowerBound)) {
                        return false;
                    }
                } else {
                    if (val <= (lowerBound)) {
                        return false;
                    }
                }
            }
            if (upperBound != null) {
                if (upperInclusive) {
                    if (val > (upperBound)) {
                        return false;
                    }
                } else {
                    if (val >= (upperBound)) {
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

    @Override
    public String toString() {
        return (lowerInclusive ? "[" : "(") +
               (lowerBound == null ? "" : lowerBound) + ", " +
               (upperBound == null ? "" : upperBound) +
               (upperInclusive ? "]" : ")");
    }

}
