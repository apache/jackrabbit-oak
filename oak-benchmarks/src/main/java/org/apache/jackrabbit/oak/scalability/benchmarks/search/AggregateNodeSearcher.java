/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.jackrabbit.oak.scalability.benchmarks.search;

import com.google.common.collect.Lists;

import org.apache.jackrabbit.api.security.user.Authorizable;

import javax.annotation.Nonnull;
import javax.jcr.*;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;

import java.util.List;
import java.util.Random;

import org.apache.jackrabbit.oak.scalability.suites.ScalabilityAbstractSuite;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityNodeRelationshipSuite;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityAbstractSuite.ExecutionContext;

/**
 * Retrieves search property by iterating over nodes and then executes search using the retrieved
 * criteria.
 */
public class AggregateNodeSearcher extends SearchScalabilityBenchmark {
    private static final String RELATIONSHIPS = "relationships";

    /**
     * Queries for nodes with property satisfying a set of properties and ordering by the latest.
     *
     * @param qm the query manager
     * @param context the execution context
     * @return the query object
     * @throws RepositoryException
     */
    protected Query getQuery(@Nonnull QueryManager qm,
        ExecutionContext context) throws RepositoryException {
        List<String> relationships = (List<String>) context.getMap().get(RELATIONSHIPS);
        // /jcr:root//element(*, ActivityType)[((id = 1234 or id = '1354'))] order by jcr:created
        // descending
        StringBuilder statement = new StringBuilder("");
        statement.append("/jcr:root")
            .append("//element(*, ")
            .append(
                (String) context.getMap().get(ScalabilityNodeRelationshipSuite.CTX_ACT_NODE_TYPE_PROP))
            .append(")");
        statement.append("[((");

        // adding all the possible mime-types in an OR fashion
        for (String relationship : relationships) {
            statement.append(ScalabilityNodeRelationshipSuite.SOURCE_ID).append(" = '")
                .append(relationship).append("' or ");
        }

        // removing latest ' or '
        statement.delete(statement.lastIndexOf(" or "), statement.length());

        statement.append("))]");
        // order by jcr:created descending
        statement.append(" order by").append(" @").append(ScalabilityNodeRelationshipSuite.CREATED)
            .append(" descending");

        LOG.debug("{}", statement);

        return qm.createQuery(statement.toString(), Query.XPATH);
    }

    @Override
    public void execute(Repository repository, Credentials credentials,
        ExecutionContext context) throws Exception {
        Session session = repository.login(credentials);
        QueryManager qm;
        try {
            List<Authorizable> users = (List<Authorizable>) context.getMap()
                .get(ScalabilityNodeRelationshipSuite.CTX_USER);
            Random rand = new Random(99);
            Authorizable user = users.get(rand.nextInt(users.size()));
            List<String> targets = getRelatedUsers(session, user);
            context.getMap().put(RELATIONSHIPS, targets);
            qm = session.getWorkspace().getQueryManager();
            search(qm, context);
            context.getMap().remove(RELATIONSHIPS);
        } catch (RepositoryException e) {
            e.printStackTrace();
        }
    }

    private List<String> getRelatedUsers(Session session, Authorizable user)
        throws RepositoryException {
        List<String> targets = Lists.newArrayList();
        Node relRootNode =
            session.getNode(user.getPath() + "/" + ScalabilityNodeRelationshipSuite.RELATIONSHIPS);
        NodeIterator children = relRootNode.getNodes();
        while (children.hasNext()) {
            Node node = children.nextNode();
            targets.add(node.getProperty(ScalabilityNodeRelationshipSuite.TARGET_ID).getString());
        }
        return targets;
    }
}
