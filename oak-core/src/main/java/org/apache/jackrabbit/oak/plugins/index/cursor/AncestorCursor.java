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
package org.apache.jackrabbit.oak.plugins.index.cursor;

import java.util.Iterator;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryLimits;
import org.jetbrains.annotations.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

/**
 * This class allows to iterate over the parent nodes of the wrapped cursor.
 */
class AncestorCursor extends PathCursor {

    public AncestorCursor(Cursor cursor, int level, QueryLimits settings) {
        super(transform(cursor, level), true, settings);
    }

    private static Iterator<String> transform(Cursor cursor, final int level) {
        Iterator<String> unfiltered = Iterators.transform(cursor,
                new Function<IndexRow, String>() {
            @Override
            public String apply(@Nullable IndexRow input) {
                return input != null ? input.getPath() : null;
            }
        });
        Iterator<String> filtered = Iterators.filter(unfiltered,
                new Predicate<String>() {
            @Override
            public boolean apply(@Nullable String input) {
                return input != null && PathUtils.getDepth(input) >= level;
            }
        });
        return Iterators.transform(filtered, new Function<String, String>() {
            @Override
            public String apply(String input) {
                return PathUtils.getAncestorPath(input, level);
            }
        });
    }
}