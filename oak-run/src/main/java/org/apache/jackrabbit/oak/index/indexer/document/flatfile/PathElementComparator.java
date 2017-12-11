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

package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

public class PathElementComparator implements Comparator<Iterable<String>> {
    private final Set<String> preferred;

    public PathElementComparator() {
        this(Collections.emptySet());
    }

    public PathElementComparator(Iterable<String> preferredPathElements) {
        this.preferred = ImmutableSet.copyOf(preferredPathElements);
    }

    @Override
    public int compare(Iterable<String> p1, Iterable<String> p2) {
        Iterator<String> i1 = p1.iterator();
        Iterator<String> i2 = p2.iterator();

        //Shorter paths come first i.e. first parent then children
        //Also Rank jcr:content higher i.e. first child
        while (i1.hasNext() || i2.hasNext()) {
            if (!i1.hasNext()) {
                return -1;
            }
            if (!i2.hasNext()) {
                return 1;
            }

            String pe1 = i1.next();
            String pe2 = i2.next();

            boolean pe1Preferred = preferred.contains(pe1);
            boolean pe2Preferred = preferred.contains(pe2);

            if (pe1Preferred && !pe2Preferred) {
                return -1;
            }

            if (pe2Preferred && !pe1Preferred) {
                return 1;
            }

            int compare = pe1.compareTo(pe2);
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }
}
