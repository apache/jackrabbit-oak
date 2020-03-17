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
package org.apache.jackrabbit.oak.jcr.xml;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import javax.jcr.ImportUUIDBehavior;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;

import org.apache.jackrabbit.test.AbstractJCRTest;
import org.junit.Assert;

/**
 * @see <a href="https://issues.apache.org/jira/browse/OAK-3930">OAK-3930</a>
 */
public class ImportMvPropertyTest extends AbstractJCRTest {

    private ValueFactory vf;

    private String path;
    private String targetPath;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        vf = superuser.getValueFactory();

        Node node = testRootNode.addNode(nodeName4);
        path = node.getPath();

        Node n1 = node.addNode(nodeName1, "test:setProperty");
        n1.setProperty("test:multiProperty", "v1");

        Node n2 = node.addNode(nodeName2, "test:setProperty");
        n2.setProperty("test:multiProperty", new String[] {"v1", "v2"});

        Node n3 = node.addNode(nodeName3, "test:setProperty");
        n3.setProperty("test:multiProperty", new String[] {});

        targetPath = testRootNode.addNode("target").getPath();

        superuser.save();

    }

    private InputStream getImportStream() throws RepositoryException, IOException {
        OutputStream out = new ByteArrayOutputStream();
        superuser.exportSystemView(path, out, true, false);
        return new ByteArrayInputStream(out.toString().getBytes());
    }

    public void testSingleValues() throws Exception {
        superuser.importXML(targetPath, getImportStream(), ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW);
        superuser.save();

        Node n = superuser.getNode(targetPath).getNode(nodeName4);
        Property p = n.getNode(nodeName1).getProperty("test:multiProperty");

        assertTrue(p.isMultiple());
        assertTrue(p.getDefinition().isMultiple());
        Assert.assertArrayEquals(new Value[]{vf.createValue("v1")}, p.getValues());
    }

    public void testMultiValues() throws Exception {
        superuser.importXML(targetPath, getImportStream(), ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW);
        superuser.save();

        Node n = superuser.getNode(targetPath).getNode(nodeName4);
        Property p = n.getNode(nodeName2).getProperty("test:multiProperty");

        assertTrue(p.isMultiple());
        assertTrue(p.getDefinition().isMultiple());
        Value[] expected = new Value[] {vf.createValue("v1"), vf.createValue("v2")};
        Assert.assertArrayEquals(expected, p.getValues());
    }

    public void testEmptyValues() throws Exception {
        superuser.importXML(targetPath, getImportStream(), ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW);
        superuser.save();

        Node n = superuser.getNode(targetPath).getNode(nodeName4);
        Property p = n.getNode(nodeName3).getProperty("test:multiProperty");

        assertTrue(p.isMultiple());
        assertTrue(p.getDefinition().isMultiple());
        Assert.assertArrayEquals(new String[0], p.getValues());
    }
}