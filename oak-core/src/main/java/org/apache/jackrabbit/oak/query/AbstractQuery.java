/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.jackrabbit.oak.query;

import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.query.ast.ColumnImpl;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A "select" or "union" query.
 */
public interface AbstractQuery {

    void setRootTree(Tree rootTree);

    void setRootState(NodeState rootState);

    void setNamePathMapper(NamePathMapper namePathMapper);

    void setLimit(long limit);

    void setOffset(long offset);

    void bindValue(String key, PropertyValue value);

    void setQueryEngine(QueryEngineImpl queryEngineImpl);

    void prepare();

    Result executeQuery();

    List<String> getBindVariableNames();

    ColumnImpl[] getColumns();

    List<SelectorImpl> getSelectors();

    Iterator<ResultRowImpl> getRows();

    long getSize();

}
