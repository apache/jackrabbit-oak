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

import static org.apache.jackrabbit.oak.commons.PathUtils.isAbsolute;

import java.util.HashSet;
import java.util.Iterator;

import org.apache.jackrabbit.oak.query.FilterIterators;
import org.apache.jackrabbit.oak.query.index.IndexRowImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryLimits;
import org.jetbrains.annotations.Nullable;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

/**
 * <code>PathCursor</code> implements a simple {@link Cursor} that iterates
 * over a {@link String} based path {@link Iterable}.
 */
public class PathCursor extends AbstractCursor {

    private final Iterator<String> iterator;

    public PathCursor(Iterator<String> paths, boolean distinct, final QueryLimits settings) {
        Iterator<String> it = paths;
        if (distinct) {
            it = Iterators.filter(it, new Predicate<String>() {
                
                private final HashSet<String> known = new HashSet<String>();

                @Override
                public boolean apply(@Nullable String input) {
                    FilterIterators.checkMemoryLimit(known.size(), settings);
                    // Set.add returns true for new entries
                    return known.add(input);
                }
                
            });
        }
        this.iterator = it;
    }

    @Override
    public IndexRow next() {
        // TODO support jcr:score and possibly rep:excerpt
        String path = iterator.next();
        return new IndexRowImpl(isAbsolute(path) ? path : "/" + path);
    }
    
    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }
    
}