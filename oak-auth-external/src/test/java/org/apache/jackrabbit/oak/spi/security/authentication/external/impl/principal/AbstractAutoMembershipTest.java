package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.AbstractExternalAuthTest;
import org.junit.After;
import org.junit.Before;

import java.util.Map;

public abstract class AbstractAutoMembershipTest  extends AbstractExternalAuthTest {

    static final String IDP_VALID_AM = "idp1";
    static final String IDP_INVALID_AM = "idp2";
    static final String IDP_MIXED_AM = "idp3";
    
    static final String AUTOMEMBERSHIP_GROUP_ID_1 = "autoMembershipGroupId_1";
    static final String AUTOMEMBERSHIP_GROUP_ID_2 = "autoMembershipGroupId_2";
    static final String NON_EXISTING_GROUP_ID = "nonExistingGroupId";
    
    static final Map<String, String[]> MAPPING = ImmutableMap.of(
            IDP_VALID_AM, new String[] {AUTOMEMBERSHIP_GROUP_ID_1, AUTOMEMBERSHIP_GROUP_ID_2},
            IDP_INVALID_AM, new String[] {NON_EXISTING_GROUP_ID},
            IDP_MIXED_AM, new String[] {AUTOMEMBERSHIP_GROUP_ID_1, NON_EXISTING_GROUP_ID});

    UserManager userManager;
    Group automembershipGroup1;
    Group automembershipGroup2;

    @Before
    public void before() throws Exception {
        super.before();
        userManager = getUserManager(root);
        automembershipGroup1 = userManager.createGroup(AUTOMEMBERSHIP_GROUP_ID_1);
        automembershipGroup2 = userManager.createGroup(AUTOMEMBERSHIP_GROUP_ID_2);
        root.commit();
    }

    @After
    public void after() throws Exception {
        try {
            if (automembershipGroup1 != null) {
                automembershipGroup1.remove();
            }
            if (automembershipGroup2 != null) {
                automembershipGroup2.remove();
            }
            root.commit();
        } finally {
            super.after();
        }
    }
}
