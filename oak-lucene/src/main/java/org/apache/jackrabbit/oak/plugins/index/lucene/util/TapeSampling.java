/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.lucene.util;

import org.apache.jackrabbit.guava.common.collect.AbstractIterator;
import org.apache.jackrabbit.oak.commons.conditions.Validate;

import java.util.Iterator;
import java.util.Random;

/**
 * @deprecated Class moved to package org.apache.jackrabbit.oak.plugins.index.search.util in oak-search
 */
@Deprecated
public class TapeSampling<T> {
    private final Random rGen;
    private final Iterator<T> input;
    private final int N;
    private final int k;

    public TapeSampling(final Random rGen, final Iterator<T> input, final int N, final int k) {
        this.rGen = rGen;
        this.input = input;
        this.N = N;
        this.k = k;
    }

    public Iterator<T> getSamples() {
        return new AbstractIterator<T>() {
            int sampled = 0;
            int seen = 0;

            @Override
            protected T computeNext() {
                if (sampled == k) {
                    return endOfData();
                }

                while (true) {
                    Validate.checkArgument(input.hasNext(),
                            "Not enough input items provided. Declared: " + N + "; got " + seen + "; sampled: " + sampled);

                    T i = input.next();

                    int r = rGen.nextInt(N - seen) + 1;
                    seen++;

                    if (r <= k - sampled) {
                        sampled++;
                        return i;
                    }
                }
            }
        };
    }
}
