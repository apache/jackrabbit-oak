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

package org.apache.jackrabbit.oak.segment.file;

import javax.annotation.Nonnull;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;

class Reclaimers {

    private Reclaimers() {
        // Prevent instantiation.
    }

    static Predicate<GCGeneration> newOldReclaimer(@Nonnull final GCGeneration reference, int retainedGenerations) {
        return new Predicate<GCGeneration>() {

            @Override
            public boolean apply(GCGeneration generation) {
                int deltaFull = reference.compareFull(generation);

                if (deltaFull == 0) {
                    if (generation.getTail() == 0) {
                        return false;
                    }

                    int deltaTail = reference.compareTail(generation);

                    if (deltaTail == 0) {
                        return false;
                    }

                    if (deltaTail >= retainedGenerations) {
                        return !generation.isTail();
                    }

                    return false;
                }

                if (deltaFull >= retainedGenerations) {
                    return true;
                }

                return generation.getTail() != 0;
            }

            @Override
            public String toString() {
                return String.format(
                        "(generation older than %d.%d, with %d retained generations)",
                        reference.getFull(),
                        reference.getTail(),
                        retainedGenerations
                );
            }

        };
    }

    static Predicate<GCGeneration> newExactReclaimer(@Nonnull final GCGeneration failedGeneration) {
        return new Predicate<GCGeneration>() {
            @Override
            public boolean apply(GCGeneration generation) {
                return generation.equals(failedGeneration);
            }
            @Override
            public String toString() {
                return "(generation==" + failedGeneration + ")";
            }
        };
    }

}
