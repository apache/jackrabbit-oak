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
package org.apache.jackrabbit.oak.query;

/**
 * A query result row that keeps all data in memory.
 */
public class Row implements Comparable<Row> {

    private final Query qom;
    private final String[] paths;
    private final Value[] values;
    private final Value[] orderValues;

    Row(Query qom, String[] paths, Value[] values, Value[] orderValues) {
        this.qom = qom;
        this.paths = paths;
        this.values = values;
        this.orderValues = orderValues;
    }

    public String getPath() {
        if (paths.length > 1) {
            throw new IllegalArgumentException("More than one selector");
        }
        return paths[0];
    }

    public String getPath(String selectorName) {
        return paths[qom.getSelectorIndex(selectorName)];
    }

    public Value getValue(String columnName) {
        return values[qom.getColumnIndex(columnName)];
    }

    public Value[] getValues() {
        Value[] v2 = new Value[values.length];
        System.arraycopy(values, 0, v2, 0, v2.length);
        return v2;
    }

    @Override
    public int compareTo(Row o) {
        return qom.compareRows(orderValues, o.orderValues);
    }

}
