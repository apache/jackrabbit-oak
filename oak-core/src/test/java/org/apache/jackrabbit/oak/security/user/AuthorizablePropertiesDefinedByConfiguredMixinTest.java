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
package org.apache.jackrabbit.oak.security.user;

import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NODE_TYPES_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 * OAK-9675 - Tests to verify that when authorizablePropertiesMixins configuration defines
 * allowed mixin types, that the properties defined by those mixin types are
 * accessible via the authorizable properties apis.
 */
public class AuthorizablePropertiesDefinedByConfiguredMixinTest extends AbstractSecurityTest {

    private AuthorizablePropertiesImpl properties;

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        // define the mixin types that are allowed to be the source for authorizable properties
        return ConfigurationParameters.of(UserConfiguration.NAME,
                ConfigurationParameters.of(
                        UserConstants.PARAM_AUTHORIZABLE_PROPERTIES_MIXINS, "mix:language")
        );
    }

    @Override
    public void before() throws Exception {
        super.before();

        User user = getTestUser();

        // declare the mixin
        Tree authorizableNode = root.getTree(user.getPath());
        String userId = user.getID();
        TreeUtil.addMixin(authorizableNode, "mix:language", root.getTree(NODE_TYPES_PATH), userId);

        // define a property that is defined by the mixin type
        ValueFactory vf = getValueFactory(root);
        user.setProperty(JcrConstants.JCR_LANGUAGE, vf.createValue("en"));

        if (root.hasPendingChanges()) {
            root.commit();
        }

        properties = new AuthorizablePropertiesImpl((AuthorizableImpl) user, getPartialValueFactory());
    }

    /**
     * Verify that the mixin defined property is accessible
     */
    @Test
    public void testHasPropertyDeclaredByMixinType() throws RepositoryException {
        assertTrue(properties.hasProperty(JcrConstants.JCR_LANGUAGE));
    }

    /**
     * Verify that the mixin defined property is accessible
     */
    @Test
    public void testGetPropertyDeclaredByMixinType() throws RepositoryException {
        Value[] property = properties.getProperty(JcrConstants.JCR_LANGUAGE);
        assertNotNull(property);
        assertEquals(1, property.length);
        assertEquals("en", property[0].getString());
    }

    /**
     * Verify that the mixin defined property is accessible
     */
    @Test
    public void testAuthorizableHasPropertyDeclaredByMixinType() throws Exception {
        User user = getTestUser();
        assertTrue(user.hasProperty(JcrConstants.JCR_LANGUAGE));
    }

    /**
     * Verify that the mixin defined property names are included in the 
     * authorizable property names
     */
    @Test
    public void testAuthorizableGetPropertyNamesIncludesPropertyDeclaredByMixinType() throws Exception {
        User user = getTestUser();
        @NotNull
        Iterator<String> names = user.getPropertyNames();
        assertNotNull(names);
        boolean foundJcrLanguage = false;
        while (names.hasNext()) {
            String name = names.next();
            if (JcrConstants.JCR_LANGUAGE.equals(name)) {
                foundJcrLanguage = true;
            }
        }
        assertTrue(foundJcrLanguage);
    }

}
