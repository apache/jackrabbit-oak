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
package org.apache.jackrabbit.oak.plugins.index.property;


/**
 * convenience orderable object that represents a tuple of values and paths
 * 
 * where the values are the indexed keys from the index and the paths are the path which hold the
 * key
 */
public class ValuePathTuple implements Comparable<ValuePathTuple> {
    private String value;
    private String path;

    ValuePathTuple(String value, String path) {
        this.value = value;
        this.path = path;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + this.getClass().hashCode();
        result = prime * result + ((path == null) ? 0 : path.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ValuePathTuple other = (ValuePathTuple) obj;
        if (path == null) {
            if (other.getPath() != null) {
                return false;
            }
        } else if (!path.equals(other.getPath())) {
            return false;
        }
        if (value == null) {
            if (other.getValue() != null) {
                return false;
            }
        } else if (!value.equals(other.getValue())) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(ValuePathTuple o) {
        if (this.equals(o)) {
            return 0;
        }
        if (this.value.compareTo(o.getValue()) < 0) {
            return -1;
        }
        if (this.value.compareTo(o.getValue()) > 0) {
            return 1;
        }
        if (this.path.compareTo(o.getPath()) < 0) {
            return -1;
        }
        if (this.path.compareTo(o.getPath()) > 0) {
            return 1;
        }
        return 0;
    }

    public String getValue() {
        return value;
    }

    public String getPath() {
        return path;
    }
}