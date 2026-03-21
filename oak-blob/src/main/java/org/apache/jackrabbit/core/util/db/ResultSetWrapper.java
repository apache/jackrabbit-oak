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
package org.apache.jackrabbit.core.util.db;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;


/**
 * This is a dynamic proxy in order to support both Java 5 and 6.
 */
public final class ResultSetWrapper implements InvocationHandler {

    private final Connection connection;

    private final Statement statement;

    private final ResultSet resultSet;

    /**
     * Creates a new {@code ResultSet} proxy which closes the given {@code Connection} and
     * {@code Statement} if it is closed.
     * 
     * @param con the associated {@code Connection}
     * @param stmt the associated {@code Statement}
     * @param rs the {@code ResultSet} which backs the proxy
     * @return a {@code ResultSet} proxy
     */
    public static final ResultSet newInstance(Connection con, Statement stmt, ResultSet rs) {
        ResultSetWrapper proxy = new ResultSetWrapper(con, stmt, rs);
        return (ResultSet) Proxy.newProxyInstance(rs.getClass().getClassLoader(),
            new Class<?>[]{ResultSet.class}, proxy);
    }

    private ResultSetWrapper(Connection con, Statement stmt, ResultSet rs) {
        connection = con;
        statement = stmt;
        resultSet = rs;
    }

    /**
     * {@inheritDoc}
     */
    public Object invoke(Object proxy, Method m, Object[] args) throws Throwable {
        if ("close".equals(m.getName())) {
            DbUtility.close(connection, statement, resultSet);
            return null;
        } else {
            return m.invoke(resultSet, args);
        }
    }
}
