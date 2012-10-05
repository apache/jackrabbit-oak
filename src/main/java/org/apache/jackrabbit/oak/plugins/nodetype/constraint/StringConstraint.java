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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.jcr.RepositoryException;
import javax.jcr.Value;

import com.google.common.base.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringConstraint implements Predicate<Value> {
    private static final Logger log = LoggerFactory.getLogger(StringConstraint.class);

    private final Pattern pattern;

    public StringConstraint(String definition) {
        Pattern p;
        try {
            p = Pattern.compile(definition);
        }
        catch (PatternSyntaxException pse) {
            String msg = '\'' + definition + "' is not valid regular expression syntax";
            log.warn(msg);
            p = null;
        }
        pattern = p;
    }

    @Override
    public boolean apply(Value value) {
        if (value == null) {
            return false;
        }

        try {
            Matcher matcher = pattern.matcher(value.getString());
            return matcher.matches();
        }
        catch (RepositoryException e) {
            log.warn("Error checking string constraint " + this, e);
            return false;
        }
    }

    @Override
    public String toString() {
        return "'" + pattern + '\'';
    }
}
