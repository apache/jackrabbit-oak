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

package org.apache.jackrabbit.oak.plugins.document.bundlor;

import java.util.List;

import com.google.common.collect.Lists;

import static com.google.common.base.Preconditions.checkArgument;

class CompositeMatcher implements Matcher {
    private final List<Matcher> matchers;

    public static Matcher compose(List<Matcher> matchers){
        switch (matchers.size()) {
            case 0:
                return Matcher.NON_MATCHING;
            case 1:
                return matchers.get(0);
            default:
                return new CompositeMatcher(matchers);
        }
    }

    /**
     * A CompositeMatcher must only be constructed when all passed
     * matchers are matching
     */
    private CompositeMatcher(List<Matcher> matchers) {
        for (Matcher m : matchers){
            checkArgument(m.isMatch(), "Non matching matcher found in [%s]", matchers);
        }
        this.matchers = matchers;
    }

    @Override
    public Matcher next(String name) {
        List<Matcher> nextSet = Lists.newArrayListWithCapacity(matchers.size());
        for (Matcher current : matchers){
            Matcher next = current.next(name);
            if (next.isMatch()){
                nextSet.add(next);
            }
        }
        return compose(nextSet);
    }

    @Override
    public boolean isMatch() {
        return true;
    }

    @Override
    public String getMatchedPath() {
        //All matchers would have traversed same path. So use any one to
        //determine the matching path
        return matchers.get(0).getMatchedPath();
    }

    @Override
    public int depth() {
        return matchers.get(0).depth();
    }

    @Override
    public boolean matchesAllChildren() {
        for (Matcher m : matchers){
            if (m.matchesAllChildren()){
                return true;
            }
        }
        return false;
    }
}
