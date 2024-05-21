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
package org.apache.jackrabbit.oak.jcr.query.qom;

import javax.jcr.query.qom.QueryObjectModelConstants;

/**
 * Enumeration of the JCR 2.0 query order.
 *
 * @since Apache Jackrabbit 2.0
 */
public enum Order {

    ASCENDING(QueryObjectModelConstants.JCR_ORDER_ASCENDING),

    DESCENDING(QueryObjectModelConstants.JCR_ORDER_DESCENDING);

    /**
     * JCR name of this order.
     */
    private final String name;

    Order(String name) {
        this.name = name;
    }

    /**
     * @return the JCR name of this order.
     */
    public String getName() {
        return name;
    }

    /**
     * Return the order with the given JCR name.
     *
     * @param name the JCR name of an order.
     * @return the order with the given name.
     * @throws IllegalArgumentException if {@code name} is not a known JCR order name.
     */
    public static Order getOrderByName(String name) {
        for (Order order : Order.values()) {
            if (order.name.equals(name)) {
                return order;
            }
        }
        throw new IllegalArgumentException("Unknown order name: " + name);
    }
}
