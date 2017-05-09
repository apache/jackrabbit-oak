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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.Collections;
import java.util.Comparator;

/**
 * <code>StableRevisionComparator</code> implements a revision comparator, which
 * is only based on stable information available in the two revisions presented
 * to this comparator. This class is used in sorted collections where
 * revision keys must have a stable ordering independent from the time when
 * a revision was seen.
 * <p>
 * Revisions are first ordered by timestamp, then counter and finally cluster
 * node id.
 */
public class StableRevisionComparator implements Comparator<Revision> {

    public static final Comparator<Revision> INSTANCE = new StableRevisionComparator();

    public static final Comparator<Revision> REVERSE = Collections.reverseOrder(INSTANCE);

    private StableRevisionComparator() {
    }

    @Override
    public int compare(Revision o1, Revision o2) {
        return o1.compareTo(o2);
    }
}
