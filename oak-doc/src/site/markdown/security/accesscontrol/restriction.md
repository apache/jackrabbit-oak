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
management in order to allow for further refinement of individual policy entries.

Quoting from JSR 283 section 16.6.2 Permissions:

> [...] the permissions encompass the restrictions imposed by privileges, but
> also include any additional policy-internal refinements with effects too
> fine-grained to be exposed through privilege discovery. A common case may be
> to provide finer-grained access restrictions to individual properties or
> child nodes of the node to which the policy applies.

Furthermore the restriction concept is aimed to allow for custom extensions of the
default access control implementation to meet project specific needs without
having to implement the common functionality provided by JCR.

Existing and potential examples of the restriction concept to limit the effect of
a given access control entry include:

- set of node types
- set of namespaces
- name/path pattern
- dedicated time frame
- size of a value

While few examples have been present with Jackrabbit 2.x the set of built-in
restrictions has been extended as of Oak 1.0 along with some useful extensions
of the Jackrabbit API. In addition Oak provides it's own public restriction
API that add support for internal validation and evaluation.

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


### Oak Restriction API

The following public interfaces are provided by Oak in the package `org.apache.jackrabbit.oak.spi.security.authorization.restriction`:

- [RestrictionProvider]
- [Restriction]
- [RestrictionDefinition]
- [RestrictionPattern]

### Default Implementation

Oak 1.0 provides the following base implementations:

- `AbstractRestrictionProvider`: abstract base implementation of the provider interface.
- `RestrictionDefinitionImpl`: default implementation of the `RestrictionDefinition` interface.
- `RestrictionImpl`: default implementation of the `Restriction` interface.
- `CompositeRestrictionProvider`: Allows to aggregate multiple provider implementations.
- `CompositePattern`: Allows to aggregate multiple restriction patterns.

#### Changes wrt Jackrabbit 2.x

Apart from the fact that the internal Jackrabbit extension has been replaced by
a public API, the restriction implementation in Oak differs from Jackrabbit 2.x
as follows:

- supports multi-valued restrictions
- validation of the restrictions is delegated to a dedicated commit hook
- restriction `rep:glob` limits the number of wildcard characters to 20
- new restrictions `rep:ntNames` and `rep:prefixes`

#### Built-in Restrictions

The default implementations of the `Restriction` interface are present with
Oak 1.0 access control management:

* `rep:glob`: single name or path pattern with '*' wildcard(s).
* `rep:ntNames`: multivalued restriction for primary node type names (no inheritence, since Oak 1.0)
* `rep:prefixes`: multivalued restriction for namespace prefixes (session level remapping not respected, since Oak 1.0)


### Pluggability

The default security setup as present with Oak 1.0 is able to provide custom
`RestrictionProvider` implementations and will automatically combine the
different implementations using the `CompositeRestrictionProvider`.

In an OSGi setup the following steps are required in order to add a action provider
implementation:

- implement `RestrictionProvider` interface exposing your custom restriction(s).
- make the provider implementation an OSGi service and make it available to the Oak repository.

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
