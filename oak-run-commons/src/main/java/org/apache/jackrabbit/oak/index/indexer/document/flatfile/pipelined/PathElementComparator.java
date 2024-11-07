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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import java.util.Comparator;
import java.util.Set;
import java.util.stream.Collectors;

public class PathElementComparator implements Comparator<String[]> {
    private final Set<String> preferred;

    public PathElementComparator(Set<String> preferredPathElements) {
        // Many of the lookups in this set will be for interned Strings, so interning the elements will speed-up lookups
        // for Strings that are present in the set, because the call to String::equals done by the Set implementation
        // will likely be comparing the same String object, so it can return after the reference equality check, avoiding
        // having to do a comparison of the contents.
        this.preferred = preferredPathElements.stream().map(String::intern).collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public int compare(String[] p1, String[] p2) {
        int i1 = 0;
        int i2 = 0;

        //Shorter paths come first i.e. first parent then children
        //Also Rank jcr:content higher i.e. first child
        while (i1 < p1.length || i2 < p2.length) {
            if (i1 >= p1.length) {
                return -1;
            }
            if (i2 >= p2.length) {
                return 1;
            }

            String pe1 = p1[i1]; i1++;
            String pe2 = p2[i2]; i2++;

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
