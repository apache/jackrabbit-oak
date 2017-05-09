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

package org.apache.jackrabbit.oak.plugins.index.lucene;

public enum IndexFormatVersion {
    /**
     * Index confirming to Oak version upto 1.0.8
     */
    V1(1),
    /**
     * Index confirming to Oak version upto 1.0.9
     */
    V2(2);

    private final int version;

    IndexFormatVersion(int version) {
        this.version = version;
    }

    /**
     * Returns <code>true</code> if this version is at least as high as the
     * given <code>version</code>.
     *
     * @param version the other version to compare.
     * @return <code>true</code> if this version is at least as high as the
     *         provided; <code>false</code> otherwise.
     */
    public boolean isAtLeast(IndexFormatVersion version) {
        return this.version >= version.getVersion();
    }

    public int getVersion() {
        return version;
    }

    public static IndexFormatVersion getVersion(int version) {
        switch(version){
            case 1 : return V1;
            case 2 : return V2;
            default : throw new IllegalArgumentException("Unknown version : " + version);
        }
    }

    public static IndexFormatVersion getDefault(){
        return V2;
    }

    public static IndexFormatVersion max(IndexFormatVersion o1, IndexFormatVersion o2){
        return o1.version > o2.version ? o1 : o2;
    }

}
