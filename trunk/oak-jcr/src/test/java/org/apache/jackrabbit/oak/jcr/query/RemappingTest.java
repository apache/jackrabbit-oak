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
package org.apache.jackrabbit.oak.jcr.query;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;

import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.test.AbstractJCRTest;

/**
 * Namespace remapping test
 */
public class RemappingTest extends AbstractJCRTest {

    private Session session;
    private String resultPath;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        superuser.getWorkspace().getNamespaceRegistry().registerNamespace("qTest", "http://jackrabbit-oak-2.apache.org");

        Node n = testRootNode.addNode("qTest:node").addNode("qTest:node2").addNode("qTest:node3");
        n.setProperty("qTest:property", superuser.getValueFactory().createValue("stringValue"));
        n.setProperty("qTest:booleanProperty", superuser.getValueFactory().createValue(true));
        n.setProperty("qTest:nameProperty", superuser.getValueFactory().createValue("qTest:nameValue", PropertyType.NAME));
        superuser.save();

        session = getHelper().getSuperuserSession();
        session.setNamespacePrefix("my", "http://jackrabbit-oak-2.apache.org");
        session.setNamespacePrefix("myRep", NamespaceConstants.NAMESPACE_REP);
        resultPath = testRootNode.getPath() + "/my:node/my:node2/my:node3";
    }

    @Override
    protected void tearDown() throws Exception {
        session.logout();
        super.tearDown();
    }

    public void testQuery1() throws Exception {
        String statement = createStatement("my:property", "stringValue");

        QueryManager qm = session.getWorkspace().getQueryManager();
        QueryResult qr = qm.createQuery(statement, "xpath").execute();
        
        // xpath: 
        // /jcr:root/testroot/my:node//element(*)[@my:property='stringValue']
        // select [jcr:path], [jcr:score], * from [nt:base] as a 
        // where [my:property] = 'stringValue' 
        // and isdescendantnode(a, '/testroot/my:node') 
        
        NodeIterator ni = qr.getNodes();
        assertTrue(ni.hasNext());
        assertEquals(resultPath, ni.nextNode().getPath());
    }

    public void testQuery2() throws Exception {
        String statement = createStatement("my:booleanProperty", "true");

        QueryManager qm = session.getWorkspace().getQueryManager();
        QueryResult qr = qm.createQuery(statement, "xpath").execute();
        NodeIterator ni = qr.getNodes();
        assertTrue(ni.hasNext());
        assertEquals(resultPath, ni.nextNode().getPath());
    }

    public void testQuery3() throws Exception {
        String statement = createStatement("my:nameProperty", "my:nameValue");

        QueryManager qm = session.getWorkspace().getQueryManager();
        QueryResult qr = qm.createQuery(statement, "xpath").execute();
        NodeIterator ni = qr.getNodes();
        assertTrue(ni.hasNext());
        assertEquals(resultPath, ni.nextNode().getPath());
    }

    public void testQuery4() throws Exception {
        String statement = 
                "/jcr:root/myRep:security/myRep:authorizables//" + 
                "element(*,myRep:Authorizable)[@my:property='value']";

        QueryManager qm = session.getWorkspace().getQueryManager();
        Query q = qm.createQuery(statement, "xpath");
        
        q.getBindVariableNames();

        QueryResult qr = q.execute();
        NodeIterator ni = qr.getNodes();
        while (ni.hasNext()) {
            ni.next();
        }
        
    }

    private String createStatement(String propertyName, String value) throws RepositoryException {
        return "/jcr:root"+ testRootNode.getPath() +"/my:node//element(*)[@"+propertyName+"='"+value+"']";
    }

}