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

Restriction Management
--------------------------------------------------------------------------------

### Overview

The concept of restriction has been created as extension to JCR access control
management in order to refine the effect of individual access control entries.

Quoting from JSR 283 section 16.6.2 Permissions:

> [...] the permissions encompass the restrictions imposed by privileges, but
> also include any additional policy-internal refinements with effects too
> fine-grained to be exposed through privilege discovery. A common case may be
> to provide finer-grained access restrictions to individual properties or
> child nodes of the node to which the policy applies.

Furthermore the restriction concept is aimed to allow for custom extensions of the
default access control implementation to meet project specific needs without
having to implement the common functionality provided by JCR.

Existing and potential examples of restrictions limiting the effect of
a given access control entry during permission evaluation include:

- set of node types
- set of namespaces
- name/path pattern
- dedicated time frame
- size of a value

The set of built-in restrictions present with Jackrabbit 2.x has extended as of 
Oak 1.0 along with some extensions of the Jackrabbit API. This covers the public 
facing usage of restrictions i.e. access control management.

In addition Oak provides it's own restriction API that adds support for internal 
validation and permission evaluation.

<a name="jackrabbit_api"/>
### Jackrabbit API

The Jackrabbit API add the following extensions to JCR access control management
to read and create entries with restrictions:

- `JackrabbitAccessControlList`
    - `getRestrictionNames()` : returns the JCR names of the supported restrictions.
    - `getRestrictionType(String restrictionName)` : returns property type of a given restriction.
    - `addEntry(Principal, Privilege[], boolean, Map<String, Value>)`: the map contain the restrictions.
    - `addEntry(Principal, Privilege[], boolean, Map<String, Value>, Map<String, Value[]>)`: allows to specify both single and multivalue restrictions (since Oak 1.0, Jackrabbit API 2.8)


- `JackrabbitAccessControlEntry`
    - `getRestrictionNames()`: returns the JCR names of the restrictions present with this entry.
    - `getRestriction(String restrictionName)`: returns the restriction as JCR value.
    - `getRestrictions(String restrictionName)`: returns the restriction as array of JCR values (since Oak 1.0, Jackrabbit API 2.8).

<a name="api_extensions"/>
### Oak Restriction API

The following public interfaces are provided by Oak in the package 
`org.apache.jackrabbit.oak.spi.security.authorization.restriction` and provide 
support for pluggable restrictions both for access control management and the 
repository internal permission evaluation:

- [RestrictionProvider]: interface to obtain restriction information needed for access control and permission management
- [Restriction]: the restriction object as created using Jackrabbit access control API
- [RestrictionDefinition]: the static definition of a supported restriction
- [RestrictionPattern]: the processed restriction ready for permission evaluation

<a name="default_implementation"/>
### Default Implementation

Oak 1.0 provides the following base implementations:

- `AbstractRestrictionProvider`: abstract base implementation of the provider interface.
- `RestrictionDefinitionImpl`: default implementation of the `RestrictionDefinition` interface.
- `RestrictionImpl`: default implementation of the `Restriction` interface.
- `CompositeRestrictionProvider`: Allows to aggregate multiple provider implementations (see [Pluggability](#pluggability) below).
- `CompositePattern`: Allows to aggregate multiple restriction patterns.

#### Changes wrt Jackrabbit 2.x

Apart from the fact that the internal Jackrabbit extension has been replaced by
a public API, the restriction implementation in Oak differs from Jackrabbit 2.x
as follows:

- Separate restriction management API (see below) on the OAK level that allows to ease plugging custom restrictions.
- Changed node type definition for storing restrictions in the default implementation.
    - as of OAK restrictions are collected underneath a separate child node "rep:restrictions"
    - backwards compatible behavior for restrictions stored underneath the ACE node directly
- Support for multi-valued restrictions (see [JCR-3637](https://issues.apache.org/jira/browse/JCR-3637), [JCR-3641](https://issues.apache.org/jira/browse/JCR-3641))
- Validation of the restrictions is delegated to a dedicated commit hook
- Restriction `rep:glob` limits the number of wildcard characters to 20
- New restrictions `rep:ntNames`, `rep:prefixes` and `rep:itemNames`
   
#### Built-in Restrictions

The default implementations of the `Restriction` interface are present with
Oak 1.0 access control management:

* `rep:glob`: single name, path or path pattern with '*' wildcard(s).
* `rep:ntNames`: multivalued restriction to limit the affected ACE to nodes of the specified primary node type(s) (no nt inheritence, since Oak 1.0)
* `rep:prefixes`: multivalued restriction to limit the effect to item names that match the specified namespace prefixes (session level remapping not respected, since Oak 1.0)
* `rep:itemNames`: multivalued restriction for property or node names (since Oak 1.3.8)

##### Examples

###### Using rep:glob

For a nodePath `/foo` the following results can be expected for the different
values of `rep:glob`.

| rep:glob          | Result                                                   |
|-------------------|----------------------------------------------------------|
| ""                | matches node /foo only                                   |
| /cat              | the node /foo/cat and all it's children                  |
| /cat/             | the descendants of the node /foo/cat                     |
| cat               | the node /foocat and all it's children                   |
| cat/              | all descendants of the node /foocat                      |
| \*                | foo, siblings of foo and their descendants               |
| /\*cat            | all children of /foo whose path ends with 'cat'          |
| /\*/cat           | all non-direct descendants of /foo named 'cat'           |
| /cat\*            | all descendant path of /foo that have the direct foo-descendant segment starting with 'cat' |
| \*cat             | all siblings and descendants of foo that have a name ending with 'cat' |
| \*/cat            | all descendants of /foo and foo's siblings that have a name segment 'cat' |
| cat/\*            | all descendants of '/foocat'                             |
| /cat/\*           | all descendants of '/foo/cat'                            |
| \*cat/\*          | all descendants of /foo that have an intermediate segment ending with 'cat' |

<a name="representation"/>
### Representation in the Repository

All restrictions defined by default in a Oak repository are stored as properties 
in a dedicated `rep:restriction` child node of the target access control entry node. 
Similarly, they are represented with the corresponding permission entry.
The node type definition used to represent restriction content is as follows:

    [rep:ACE]
      - rep:principalName (STRING) protected mandatory
      - rep:privileges (NAME) protected mandatory multiple
      - rep:nodePath (PATH) protected /* deprecated in favor of restrictions */
      - rep:glob (STRING) protected   /* deprecated in favor of restrictions */
      - * (UNDEFINED) protected       /* deprecated in favor of restrictions */
      + rep:restrictions (rep:Restrictions) = rep:Restrictions protected /* since oak 1.0 */

    /**
     * @since oak 1.0
     */
    [rep:Restrictions]
      - * (UNDEFINED) protected
      - * (UNDEFINED) protected multiple


<a name="pluggability"/>
### Pluggability

The default security setup as present with Oak 1.0 is able to provide custom
`RestrictionProvider` implementations and will automatically combine the
different implementations using the `CompositeRestrictionProvider`.

In an OSGi setup the following steps are required in order to add a action provider
implementation:

- implement `RestrictionProvider` interface exposing your custom restriction(s).
- make the provider implementation an OSGi service and make it available to the Oak repository.

Please make sure to consider the following recommendations when implementing a custom `RestrictionProvider`:
- restrictions are part of the overall permission evaluation and thus may heavily impact overall read/write performance
- the hashCode generation of the base implementation (`RestrictionImpl.hashCode`) relies on `PropertyStateValue.hashCode`, which includes the internal String representation, which is not optimal for binaries (see also [OAK-5784])

##### Examples

###### Example RestrictionProvider

Simple example of a `RestrictionProvider` that defines a single time-based `Restriction`,
which is expected to have 2 values defining a start and end date, which can then be used
to allow or deny access within the given time frame.

    @Component
    @Service(RestrictionProvider.class)
    public class MyRestrictionProvider extends AbstractRestrictionProvider {

        public MyRestrictionProvider() {
            super(supportedRestrictions());
        }

        private static Map<String, RestrictionDefinition> supportedRestrictions() {
            RestrictionDefinition dates = new RestrictionDefinitionImpl("dates", Type.DATES, false);
            return Collections.singletonMap(dates.getName(), dates);
        }

        //------------------------------------------------< RestrictionProvider >---

        @Override
        public RestrictionPattern getPattern(String oakPath, Tree tree) {
            if (oakPath != null) {
                PropertyState property = tree.getProperty("dates");
                if (property != null) {
                    return DatePattern.create(property);
                }
            }
            return RestrictionPattern.EMPTY;
        }

        @Nonnull
        @Override
        public RestrictionPattern getPattern(@Nullable String oakPath, @Nonnull Set<Restriction> restrictions) {
            if (oakPath != null) {
                for (Restriction r : restrictions) {
                    String name = r.getDefinition().getName();
                    if ("dates".equals(name)) {
                        return DatePattern.create(r.getProperty());
                    }
                }
            }
            return RestrictionPattern.EMPTY;
        }

        // TODO: implementing 'validateRestrictions(String oakPath, Tree aceTree)' would allow to make sure the property contains 2 date values.
    }

###### Example RestrictionPattern

The time-based `RestrictionPattern` used by the example provider above.

    class DatePattern implements RestrictionPattern {

        private final Date start;
        private final Date end;

        private DatePattern(@Nonnull Calendar start, @Nonnull Calendar end) {
            this.start = start.getTime();
            this.end = end.getTime();
        }

        static RestrictionPattern create(PropertyState timeProperty) {
            if (timeProperty.count() == 2) {
                return new DatePattern(
                        Conversions.convert(timeProperty.getValue(Type.DATE, 0), Type.DATE).toCalendar(),
                        Conversions.convert(timeProperty.getValue(Type.DATE, 1), Type.DATE).toCalendar()
                );
            } else {
                return RestrictionPattern.EMPTY;
            }
        }

        @Override
        public boolean matches(@Nonnull Tree tree, @Nullable PropertyState property) {
            return matches();
        }

        @Override
        public boolean matches(@Nonnull String path) {
            return matches();
        }

        @Override
        public boolean matches() {
            Date d = new Date();
            return d.after(start) && d.before(end);
        }
    };

###### Example Non-OSGI Setup

    RestrictionProvider rProvider = CompositeRestrictionProvider.newInstance(new MyRestrictionProvider(), ...);
    Map<String, RestrictionProvider> authorizMap = ImmutableMap.of(PARAM_RESTRICTION_PROVIDER, rProvider);
    ConfigurationParameters config =  ConfigurationParameters.of(ImmutableMap.of(AuthorizationConfiguration.NAME, ConfigurationParameters.of(authorizMap)));
    SecurityProvider securityProvider = new SecurityProviderImpl(config));
    Repository repo = new Jcr(new Oak()).with(securityProvider).createRepository();

<!-- hidden references -->
[Restriction]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/restriction/Restriction.html
[RestrictionDefinition]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/restriction/RestrictionDefinition.html
[RestrictionPattern]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/restriction/RestrictionPattern.html
[RestrictionProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/restriction/RestrictionProvider.html
[OAK-5784]: https://issues.apache.org/jira/browse/OAK-5784
