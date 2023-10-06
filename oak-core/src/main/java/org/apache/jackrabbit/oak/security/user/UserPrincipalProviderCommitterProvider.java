package org.apache.jackrabbit.oak.security.user;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.jetbrains.annotations.NotNull;

import java.security.Principal;
import java.util.HashMap;
import java.util.Set;

public class UserPrincipalProviderCommitterProvider {

    static UserPrincipalProviderCommitterProvider instance = null;
    static HashMap<String, PrincipalCommitterThread> committerThreadMap = new HashMap<>();

    public static UserPrincipalProviderCommitterProvider getInstance() {
        if (instance == null) {
            instance = new UserPrincipalProviderCommitterProvider();
        }
        return instance;
    }

    public synchronized PrincipalCommitterThread cacheGroups(@NotNull Tree authorizableNode, @NotNull Set<Principal> groupPrincipals, long expiration, Root root) {
        String authorizableNodePath = authorizableNode.getPath();
        if (committerThreadMap.containsKey(authorizableNodePath)) {
            // One thread is already committing. return null to inform the caller that doesn't have to wait for the commit to finish
            return null;
        } else {
            PrincipalCommitterThread committerThread = new PrincipalCommitterThread(authorizableNode, groupPrincipals, expiration, root, committerThreadMap);
            committerThreadMap.put(authorizableNodePath, committerThread);
            committerThread.start();
            return committerThread;
        }
    }

    public HashMap<String, PrincipalCommitterThread> getCommitterThreadMap() {
        return committerThreadMap;
    }

}
