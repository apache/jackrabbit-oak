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
package org.apache.jackrabbit.oak.jcr.nodetype;

import static junit.framework.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.Session;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeTypeDefinition;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinitionTemplate;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.apache.jackrabbit.oak.jcr.NodeStoreFixture;
import org.junit.Test;

public class NodeTypeTest extends AbstractRepositoryTest {

    public NodeTypeTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    /**
     * Add a node to a node type that does not accept child nodes
     * See OAK-479
     */
    @Test(expected = ConstraintViolationException.class)
    public void illegalAddNode() throws Exception {
        Session session = getAdminSession();
        Node root = session.getRootNode();
        root.addNode("q1", "nt:query").addNode("q2", "nt:query");
        session.save();
    }

    @Test(expected = ConstraintViolationException.class)
    public void illegalAddNodeWithProps() throws Exception {
        Session session = getAdminSession();
        Node root = session.getRootNode();
        ValueFactory vf = session.getValueFactory();

        Node n = root.addNode("q1", "nt:query");
        n.setProperty("jcr:statement", vf.createValue("statement"));
        n.setProperty("jcr:language", vf.createValue("language"));

        Node n2 = n.addNode("q2", "nt:query");
        n2.setProperty("jcr:statement", vf.createValue("statement"));
        n2.setProperty("jcr:language", vf.createValue("language"));

        session.save();
    }

    @Test
    public void updateNodeType() throws Exception {
        Session session = getAdminSession();
        Node root = session.getRootNode();
        ValueFactory vf = session.getValueFactory();
        NodeTypeManager manager = session.getWorkspace().getNodeTypeManager();

        Node n = root.addNode("q1", "nt:query");
        n.setProperty("jcr:statement", vf.createValue("statement"));
        session.save();

        NodeTypeDefinition ntd = manager.getNodeType("nt:query");
        NodeTypeTemplate ntt = manager.createNodeTypeTemplate(ntd);

        try {
            manager.registerNodeType(ntt, true);
            // no changes to the type, so the registration should be a no-op
        } catch (ConstraintViolationException unexpected) {
            fail();
        }

        // make the (still missing) jcr:language property mandatory
        @SuppressWarnings("unchecked")
        List<PropertyDefinitionTemplate> pdts = ntt.getPropertyDefinitionTemplates();
        for (PropertyDefinitionTemplate pdt : pdts) {
            if ("jcr:language".equals(pdt.getName())) {
                pdt.setMandatory(true);
            }
        }

        try {
            manager.registerNodeType(ntt, true);
            fail();
        } catch (ConstraintViolationException expected) {
            // the registration fails because of the would-be invalid content
        }

        // add the jcr:language property so it can be made mandatory
        n.setProperty("jcr:language", vf.createValue("language"));
        session.save();

        try {
            manager.registerNodeType(ntt, true);
            // now the mandatory property exists, so the type change is OK
        } catch (ConstraintViolationException unexpected) {
            fail();
        }
    }

    @Test
    public void trivialUpdates() throws Exception {
        // test various trivial updates that should not trigger repository scans
        // whether or not the repository scan happens can not be checked directly;
        // it requires inspecting the INFO level log 

        String[] types = new String[] { "trivial1", "trivial2" };
        ArrayList<NodeTypeTemplate> ntt = new ArrayList<NodeTypeTemplate>();

        // adding node types
        Session session = getAdminSession();
        NodeTypeManager manager = session.getWorkspace().getNodeTypeManager();
        for (String t : types) {
            NodeTypeTemplate nt = manager.createNodeTypeTemplate();
            nt.setName(t);
            ntt.add(nt);
        }
        manager.registerNodeTypes(ntt.toArray(new NodeTypeTemplate[0]), false);

        // adding an optional property
        ntt = new ArrayList<NodeTypeTemplate>();
        for (String t : types) {
            NodeTypeDefinition ntd = manager.getNodeType(t);
            PropertyDefinitionTemplate opt = manager.createPropertyDefinitionTemplate();
            opt.setMandatory(false);
            opt.setName("optional");
            opt.setRequiredType(PropertyType.STRING);
            PropertyDefinitionTemplate opts = manager.createPropertyDefinitionTemplate();
            opts.setMandatory(false);
            opts.setMultiple(true);
            opts.setName("optionals");
            opts.setRequiredType(PropertyType.STRING);

            NodeTypeTemplate nt = manager.createNodeTypeTemplate(ntd);
            List pdt = nt.getPropertyDefinitionTemplates();
            pdt.add(opt);
            pdt.add(opts);
            ntt.add(nt);
        }
        manager.registerNodeTypes(ntt.toArray(new NodeTypeTemplate[0]), true);

        // make one optional property mandatory
        ntt = new ArrayList<NodeTypeTemplate>();
        for (String t : types) {
            NodeTypeDefinition ntd = manager.getNodeType(t);
            PropertyDefinitionTemplate opt = manager.createPropertyDefinitionTemplate();
            opt.setMandatory("trivial2".equals(t));
            opt.setName("optional");
            opt.setRequiredType(PropertyType.STRING);
            PropertyDefinitionTemplate opts = manager.createPropertyDefinitionTemplate();
            opts.setMandatory("trivial2".equals(t));
            opts.setMultiple(true);
            opts.setName("optionals");
            opts.setRequiredType(PropertyType.STRING);

            NodeTypeTemplate nt = manager.createNodeTypeTemplate(ntd);
            List pdt = nt.getPropertyDefinitionTemplates();
            pdt.add(opt);
            pdt.add(opts);
            ntt.add(nt);
        }
        // but update both node types
        manager.registerNodeTypes(ntt.toArray(new NodeTypeTemplate[0]), true);
    }

    @Test
    public void removeNodeType() throws Exception {
        Session session = getAdminSession();
        Node root = session.getRootNode();
        ValueFactory vf = session.getValueFactory();
        NodeTypeManager manager = session.getWorkspace().getNodeTypeManager();

        Node n = root.addNode("q1", "nt:query");
        n.setProperty("jcr:statement", vf.createValue("statement"));
        n.setProperty("jcr:language", vf.createValue("language"));
        session.save();

        try {
            manager.unregisterNodeType("nt:query");
            fail();
        } catch (ConstraintViolationException expected) {
            // this type is referenced in content, so it can't be removed
        }

        n.remove();
        session.save();

        try {
            manager.unregisterNodeType("nt:query");
            // no longer referenced in content, so removal should succeed
        } catch (ConstraintViolationException unexpected) {
            fail();
        }
    }

    @Test
    public void mixReferenceable() throws Exception {
        Session session = getAdminSession();

        Node a = session.getNode("/").addNode("a" + System.currentTimeMillis());
        a.setProperty("jcr:uuid", UUID.randomUUID().toString());      // No problem here
        session.save();

        try {
            Node b = session.getNode("/").addNode("b" + System.currentTimeMillis());
            b.addMixin(JcrConstants.MIX_REFERENCEABLE);
            b.setProperty("jcr:uuid", UUID.randomUUID().toString());  // fails as jcr:uuid is protected
            session.save();
            fail();
        } catch (ConstraintViolationException expected) { }

        Node c = session.getNode("/").addNode("c" + System.currentTimeMillis());
        c.setProperty("jcr:uuid", UUID.randomUUID().toString());      // Doesn't fail as jcr:uuid is not protected yet
        c.addMixin(JcrConstants.MIX_REFERENCEABLE);
        session.save();
    }
}
