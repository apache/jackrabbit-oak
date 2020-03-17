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

Authorizable Node Name Generation
--------------------------------------------------------------------------------

### Overview

Oak 1.0 comes with a extension to the Jackrabbit user management API that allows
to change the way how the name of an authorizable node is being generated.

As in Jackrabbit 2.x the target ID is used as name-hint by default. In order to
prevent exposing identifier related information in the path of the authorizable
node, it it's desirable to change this default behavior by plugging a different
implementation of the `AuthorizableNodeName` interface.

- `AuthorizableNodeName` : Defines the generation of the authorizable node names
   in case the user management implementation stores user information in the repository.

In the default implementation the corresponding configuration parameter is
`PARAM_AUTHORIZABLE_NODE_NAME`. The default name generator can be replace
by installing an OSGi service that implementations the `AuthorizableNodeName` interface.
In a non-OSGi setup the user configuration must be initialized with configuration
parameters that provide the custom generator implementation.

### AuthorizableNodeName API

The following public interfaces are provided by Oak in the package `org.apache.jackrabbit.oak.spi.security.user`:

- [AuthorizableNodeName]

The `AuthorizableNodeName` interface itself defines single method that allows
to generate a valid JCR name for a given authorizable ID.

#### Changes wrt Jackrabbit 2.x

- The generation of the node name is a configuration option of the default
  user management implementation.
- In an OSGi-based setup the default can be changed at runtime by plugging a
  different implementation. E.g. the `RandomAuthorizableNodeName` component
  can easily be enabled by providing the required configuration.

#### Built-in AuthorizableAction Implementations

Oak 1.0 provides the following base implementations:

- `AuthorizableNodeName.Default`: Backwards compatible implementation that
   uses the authorizable ID as name hint.
- `RandomAuthorizableNodeName`: Generating a random JCR name (see [RandomAuthorizableNodeName].java).

### Pluggability

The default security setup as present with Oak 1.0 can be run with a custom
`RandomAuthorizableNodeName` implementations.

In an OSGi setup the following steps are required in order to add a different
implementation:

- implement `AuthorizableNodeName` interface.
- make the implementation an OSGi service and make it available to the Oak repository.

##### Examples

###### Example AuthorizableNodeName

In an OSGi-based setup it's sufficient to make the service available to the repository
in order to enable this custom node name generator.

    @Component
    @Service(value = {AuthorizableNodeName.class})
    /**
     * Custom implementation of the {@code AuthorizableNodeName} interface
     * that uses a uuid as authorizable node name.
     */
    final class UUIDNodeName implements AuthorizableNodeName {

        @Override
        @Nonnull
        public String generateNodeName(@Nonnull String authorizableId) {
            return UUID.randomUUID().toString();
        }
    }

In a non-OSGi setup this custom name generator can be plugged by making it available
to the user configuration as follows:

    Map<String, Object> userParams = new HashMap<String, Object>();
    userParams.put(UserConstants.PARAM_AUTHORIZABLE_NODE_NAME, new UUIDNodeName());
    ConfigurationParameters config =  ConfigurationParameters.of(ImmutableMap.of(UserConfiguration.NAME, ConfigurationParameters.of(userParams)));
    SecurityProvider securityProvider = new SecurityProviderImpl(config));
    Repository repo = new Jcr(new Oak()).with(securityProvider).createRepository();

<!-- hidden references -->
[AuthorizableNodeName]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/user/AuthorizableNodeName.html
[RandomAuthorizableNodeName]: /oak/docs/apidocs/org/apache/jackrabbit/oak/security/user/RandomAuthorizableNodeName.html
