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

Token Authentication and Token Management
--------------------------------------------------------------------------------

### General

The token based authentication has been completely refactor in Oak and has the
following general characteristics.

- Dedicated API for managing login tokens defined in the package `org.apache.jackrabbit.oak.spi.security.authentication.token`.
- Pluggable configuration of the new token management API
- Complete separation of token based authentication into a separate `LoginModule`.

### Token Authentication

As of Oak the token based authentication is handled by a dedicated [TokenLoginModule].
It is both responsible for creating new login tokens and validating [TokenCredentials]
passed to the repository login.

This token specific login module implementation obtains the [TokenProvider] from
the security configuration as defined for the content repository. The token
management implementation present with a given repository can be changed or
extended at runtime (see section Configuration below).

#### TokenLoginModule

The `TokenLoginModule`designed to support and issue `TokenCredentials`. The
authentication phases behave as follows:

*Phase 1: Login*

- if no `TokenProvider` is available **returns `false`**
- if a `TokenProvider` has been configured it retrieves JCR credentials from the [CallbackHandler] using the [CredentialsCallback]
- in case of `TokenCredentials` validates these credentials: if it succeeds
  it pushes the users ID to the shared state and returns `true`; otherwise throws `LoginException`
- for other credentials the method returns `false`

*Phase 1: Commit*

- if phase 1 succeeded the subject is populated and the method returns `true`
- in case phase 1 did not succeed this method will test if the shared state contain
  credentials that ask for a new token being created; if this succeeds it will
  create a new instance of `TokenCredentials`, push the public attributes to the
  shared stated and update the subject with the new credentials;
  finally the commit call **returns `false`**
  
##### Example JAAS Configuration

  jackrabbit.oak {
       org.apache.jackrabbit.oak.security.authentication.token.TokenLoginModule sufficient;
       org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl required;
   };


<a name="api_extensions"/>
### Token Management API

Oak 1.0 defines the following interfaces used to manage login tokens:

- [TokenConfiguration]: Interface to obtain a `TokenProvider` instance (see section [configuration](#configuration) below).
- [TokenProvider]: Interface to read and manage login tokens.
- [TokenInfo]: Information associated with a given login token and token validity.

In addition Oak comes with a default implementation of the provider interface
that is able to aggregate multiple `TokenProvider`s:

- [CompositeTokenConfiguration]: Extension of the `CompositeConfiguration` to combined different token management implementations.
- [CompositeTokenProvider]: Aggregation of the `TokenProvider` implementations defined by the configurations contained the `CompositeTokenConfiguration`

See section [Pluggability](#pluggability) for an example.

<a name="default_implementation"/>
### Characteristics of the Default Implementation

The characteristics of the default token management implementation is
described in section [Token Management : The Default Implementation](token/default.html). 

<a name="configuration"/>
### Configuration

The configuration options of the default implementation are described in
the [Configuration](token/default.html#configuration) section.


<a name="pluggability"/>
### Pluggability

The default security setup as present with Oak 1.0 is able to deal with 
custom token management implementations and will collect multiple
implementations within `CompositeTokenConfiguration` present with the
`SecurityProvider`. The `CompositeTokenConfiguration` itself will 
combine the different `TokenProvider` implementations using the `CompositeTokenProvider`.

In an OSGi setup the following steps are required in order to add a custom
token provider implementation:

 - implement `TokenProvider` interface
 - expose the custom provider by your custom `TokenConfiguration` service
 - make the configuration available to the Oak repository.

##### Examples

###### Example TokenConfiguration

    @Component()
    @Service({TokenConfiguration.class, SecurityConfiguration.class})
    public class MyTokenConfiguration extends ConfigurationBase implements TokenConfiguration {

        public TokenConfigurationImpl() {
            super();
        }

        public TokenConfigurationImpl(SecurityProvider securityProvider) {
            super(securityProvider, securityProvider.getParameters(NAME));
        }

        @Activate
        private void activate(Map<String, Object> properties) {
            setParameters(ConfigurationParameters.of(properties));
        }

        //----------------------------------------------< SecurityConfiguration >---
        @Nonnull
        @Override
        public String getName() {
            return NAME;
        }

        //-------------------------------------------------< TokenConfiguration >---
        @Nonnull
        @Override
        public TokenProvider getTokenProvider(Root root) {
            return new MyTokenProvider(root, getParameters());
        }
    }

<!-- references -->

[TokenLoginModule]: /oak/docs/apidocs/org/apache/jackrabbit/oak/security/authentication/token/TokenLoginModule.html
[TokenCredentials]: http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-api/src/main/java/org/apache/jackrabbit/api/security/authentication/token/TokenCredentials.java
[AuthInfo]: /oak/docs/apidocs/org/apache/jackrabbit/oak/api/AuthInfo.html
[ContentSession]: /oak/docs/apidocs/org/apache/jackrabbit/oak/api/ContentSession.html
[TokenProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/token/TokenProvider.html
[TokenInfo]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/token/TokenInfo.html
[CompositeTokenConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/token/CompositeTokenConfiguration.html
[CompositeTokenProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/token/CompositeTokenProvider.html