package org.apache.jackrabbit.oak.spi.security.user;

import org.apache.jackrabbit.api.security.user.UserManager;

/**
 * Created by IntelliJ IDEA.
 * User: angela
 * Date: 8/22/12
 * Time: 3:48 PM
 * To change this template use File | Settings | File Templates.
 */
public enum Type {

    USER(UserManager.SEARCH_TYPE_USER),
    GROUP(UserManager.SEARCH_TYPE_GROUP),
    AUTHORIZABLE(UserManager.SEARCH_TYPE_AUTHORIZABLE);

    private final int userType;

    Type(int userType) {
        this.userType = userType;
    }
}
