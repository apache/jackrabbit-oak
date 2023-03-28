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
package org.apache.jackrabbit.oak.commons.collect;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.commons.iterator.AbstractLazyIterator;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class Iterators {
    
    private Iterators() {}

    /**
     * Combines two iterators into a single iterator. The returned iterator iterates across the elements of each 
     * iterator. The returned iterator does not support remove.
     * 
     * @param input1 The first iterator to concatenate
     * @param input2 The second iterator to concatenate              
     * @return An iterator wrapping the passed iterators.
     */
    @NotNull
    public static <T> Iterator<T> concat(@NotNull Iterator<? extends T> input1, @NotNull Iterator<? extends T> input2) {
        return concat(Lists.newArrayList(input1, input2));

    }

    /**
     * Combines multiple iterators into a single iterator. The returned iterator iterates across the elements of each 
     * iterator in inputs. The returned iterator does not support remove.
     *
     * @param inputs The iterators to concatenate
     * @return An iterator wrapping the passed iterators.
     */
    @NotNull
    public static <T> Iterator<T> concat(@NotNull Iterator<? extends T>... inputs) {
        switch (inputs.length) {
            case 0: return Collections.emptyIterator();
            case 1: return (Iterator<T>) inputs[0];
            default: return concat(Lists.newArrayList(inputs));
        }
    }
    
    @NotNull
    private static <T> Iterator<T> concat(@NotNull List<Iterator<? extends T>> inputs) {
        return new AbstractLazyIterator<T>() {
            final List<Iterator<? extends T>> l = inputs;
            Iterator<? extends T> it = l.remove(0);

            @Override
            protected T getNext() {
                if (!it.hasNext()) {
                    if (l.isEmpty()) {
                        it = Collections.emptyIterator();
                    } else {
                        it = l.remove(0);
                    }
                }
                if (it.hasNext()) {
                    return it.next();
                }
                return null;
            }
        };
    }
}