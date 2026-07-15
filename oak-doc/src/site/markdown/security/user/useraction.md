<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
  -->

User Actions
------------

### Overview

Oak 1.10 comes with an extension to the Jackrabbit user management API that allows
to perform additional actions or validations for user specific operations
such as

- disable (or enable) a user
- allowing a given principal to impersonate the target user
- revoke the ability to impersonate the target user for a given principal

<a name="api_extensions"></a>
### UserAction API

The following public interface is provided by Oak in the package `org.apache.jackrabbit.oak.spi.security.user.action`:

- [UserAction]

The `UserAction` interface extends from `AuthorizableAction` and itself allows to perform validations or write
additional application specific content while executing user specific operations. Therefore these actions are executed as part of the transient 
user management modifications. This contrasts to `org.apache.jackrabbit.oak.spi.commit.CommitHook`s
which in turn are only triggered once modifications are persisted.

Consequently, implementations of the `UserAction` interface are expected 
to adhere to this rule and perform transient repository operations or validation.
They must not force changes to be persisted by calling `org.apache.jackrabbit.oak.api.Root.commit()`.

Any user actions are executed with the editing session and the
target operation will fail if any of the configured actions fails (e.g. due to
insufficient permissions by the editing Oak ContentSession).

<a name="default_implementation"></a>
### Default Implementations

Oak 1.10 doesn't provide any base implementation for `UserAction`.

<a name="xml_import"></a>
### XML Import

During import the user actions are called in the same way as when the corresponding API calls are invoked.

<a name="pluggability"></a>
### Pluggability

Refer to [Authorizable Actions | Pluggability ](authorizableaction.html#Pluggability) for details on how to plug
a new user action into the system.

##### Examples

###### Example Action

This example action removes the profile nodes upon disabling the user:

    ClearProfilesAction extends AbstractAuthorizableAction implements UserAction {
    
        @Override
        public void onDisable(@NotNull User user, @Nullable String disableReason, @NotNull Root root, @NotNull NamePathMapper namePathMapper) throws RepositoryException {
            if (disableReason != null) {
                Tree t = root.getTree(user.getPath());
                if (t.exists() && t.hasChild("profiles")) {
                    t.getChild("profiles").remove();
                }
            }
        }
    }

<!-- hidden references -->
[UserAction]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/user/action/UserAction.html
