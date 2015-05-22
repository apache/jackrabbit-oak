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

### Token Management API

Oak 1.0 defines the following interfaces used to manage login tokens:

- [TokenProvider]: Interface to read and manage login tokens.
- [TokenInfo]: Information associated with a given login token.

In addition Oak comes with a default implementation of the provider interface
that is able to aggregate multiple `TokenProvider`s:

- [CompositeTokenProvider]


### Characteristics of the TokenProvider Implementation

The default implementation of the token management API stores login tokens along
with the user's home directory in the repository. Along with the hash of the
login token separated properties defining the expiration time of the token
as well as as additional properties associated with the login tokens. This
additional information may be mandatory (thus validated during the login) or
optional. The optional properties are meant to have informative value only and
will be transferred to public attributes as exposed by the [AuthInfo] present
with each [ContentSession].

#### Token Creation

The creation of a new token is triggered by valid `SimpleCredentials` passed
to the login module chain that contain an additional, empty `.token` attribute.
The default `TokenProvider` implementation will consequently generate a new
token and store it's hash along with all mandatory and informative attributes
to the new content node representing the new token.

#### Token Removal

In the default implementation a given login token (and the node associated with it)
will be removed if the authentication fails due to an expired token.

#### Resetting Expiration Time

The default `TokenProvider` implementation will automatically reset the expiration
time of a given token upon successful authentication.

#### Token Representation in the Repository

##### Content Structure

The login tokens issued for a given user are all located underneath a node
named `.tokens` that will be created by the `TokenProvider` once the first token
is created. The default implementation creates a distinct node for each login
token as described below

    testUser {
        "jcr:primaryType": "rep:User",
        ...
        ".tokens" {
            "jcr:primaryType": "rep:Unstructured",
            "2014-04-10T16.09.07.159+02.00" {
                "jcr:primaryType": "rep:Token",
                ...
            "2014-05-07T12.08.57.683+02.00" {
                "jcr:primaryType": "rep:Token",
                ...
            }
            "2014-06-25T16.00.13.018+02.00" {
                "jcr:primaryType": "rep:Token",
                ...
            }
        }
    }

##### Token Nodes

As of Oak 1.0 the login token are represented in the repository as follows:

- the token node is referenceable with the dedicated node type `rep:Token` (used to be unstructured in Jackrabbit 2.x)
- expiration and key properties are defined to be mandatory and protected
- expiration time is obtained from `PARAM_TOKEN_EXPIRATION` specified in the
  login attributes and falls back to the configuration parameter with the same
  name as specified in the configuration options of the `TokenConfiguration`.

The definition of the new built-in node type `rep:Token`:

    [rep:Token] > mix:referenceable
    - rep:token.key (STRING) protected mandatory
    - rep:token.exp (DATE) protected mandatory
    - * (UNDEFINED) protected
    - * (UNDEFINED) multiple protected

The following example illustrates the token nodes resulting from this node type
definition:

    testUser {
            "jcr:primaryType": "rep:User",
            ...
            ".tokens" {
                "2014-04-10T16.09.07.159+02.00" {
                    "jcr:primaryType": "rep:Token",
                    "jcr:uuid": "30c1f361-35a2-421a-9ebc-c781eb8a08f0",
                    "rep:token.key": "{SHA-256}afaf64dba5d862f9-1000-3e2d4e58ac16189b9f2ac95d8d5b692e61cb06db437bcd9be5c10bdf3792356a",
                    "rep:token.exp": "2014-04-11T04:09:07.159+02:00",
                    ".token.ip": "0:0:0:0:0:0:0:1%0"
                    ".token.otherMandatoryProperty": "expectedValue",
                    "referer": "http://localhost:4502/crx/explorer/login.jsp"
                    "otherInformalProperty": "somevalue"
                },
                "2014-05-07T12.08.57.683+02.00" {
                    "jcr:primaryType": "rep:Token",
                    "jcr:uuid": "c95c91e2-2e08-48ab-93db-6e7c8cdd6469",
                    "rep:token.key": "{SHA-256}b1d268c55abda258-1000-62e4c368972260576d37e6ba14a10f9f02897e42992624890e22c522220f7e54",
                    "rep:token.exp": "2014-05-08T00:08:57.683+02:00"
                },
                ...
            }
        }
    }

<a name="validation"/>
##### Validation

The consistency of this content structure both on creation and modification is
asserted by a dedicated `TokenValidator`. The corresponding errors are
all of type `Constraint` with the following codes:

| Code              | Message                                                  |
|-------------------|----------------------------------------------------------|
| 0060              | Attempt to create reserved token property in other ctx   |
| 0061              | Attempt to change existing token key                     |
| 0062              | Change primary type of existing node to rep:Token        |
| 0063              | Creation/Manipulation of tokens without using provider   |
| 0064              | Create a token outside of configured scope               |
| 0065              | Invalid location of token node                           |
| 0066              | Invalid token key                                        |
| 0067              | Mandatory token expiration missing                       |
| 0068              | Invalid location of .tokens node                         |
| 0069              | Change type of .tokens parent node                       |

### Configuration

The Oak token management comes with it's own [TokenConfiguration] which allows
to obtain a new `TokenProvider` instance with the specified configuration options.

Apart from the default configuration implementation Oak provides a public
[CompositeTokenConfiguration], which is used to combined different implementations
plugged at runtime.

#### Configuration Parameters

| Parameter                           | Type    | Default                  |
|-------------------------------------|---------|--------------------------|
| PARAM_TOKEN_EXPIRATION              | long    | 2 * 3600 * 1000 (2 hours)|
| PARAM_TOKEN_LENGTH                  | int     | 8                        |
| | | |


#### Examples

##### Example JAAS Configuration

    jackrabbit.oak {
         org.apache.jackrabbit.oak.security.authentication.token.TokenLoginModule sufficient;
         org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl required;
     };


### Pluggability

The default security setup as present with Oak 1.0 is able to provide custom
`TokenProvider` implementations and will automatically combine the
different implementations using the `CompositeTokenProvider`.

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
[CompositeTokenProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/token/CompositeTokenProvider.html