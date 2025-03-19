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

Furthermore, the restriction concept is aimed to allow for custom extensions of the
default access control implementation to meet project specific needs without
having to implement the common functionality provided by JCR.

Existing and potential examples of restrictions limiting the effect of
a given access control entry during permission evaluation include:

- set of node types
- set of namespaces
- name/path pattern
- dedicated time frame
- size of a value

The set of built-in restrictions available with Jackrabbit 2.x has been extended along with some extensions of the 
Jackrabbit API. This covers the public facing usage of restrictions i.e. access control management.

In addition, Oak provides its own internal restriction API that adds support for validation and permission evaluation.

<a name="jackrabbit_api"></a>
### Jackrabbit API

The Jackrabbit API adds the following extensions to JCR access control management
to read and create entries with restrictions:

- `JackrabbitAccessControlList`
    - `getRestrictionNames()` : returns the JCR names of the supported restrictions.
    - `getRestrictionType(String restrictionName)` : returns the property type of a given restriction.
    - `addEntry(Principal, Privilege[], boolean, Map<String, Value>)`: the map contain the restrictions.
    - `addEntry(Principal, Privilege[], boolean, Map<String, Value>, Map<String, Value[]>)`: allows to specify both single and multi-value restrictions (since Oak 1.0, Jackrabbit API 2.8)


- `JackrabbitAccessControlEntry`
    - `getRestrictionNames()`: returns the JCR names of the restrictions present with this entry.
    - `getRestriction(String restrictionName)`: returns the restriction as JCR value.
    - `getRestrictions(String restrictionName)`: returns the restriction as array of JCR values (since Oak 1.0, Jackrabbit API 2.8).

<a name="api_extensions"></a>
### Oak Restriction API

The following public interfaces are provided by Oak in the package 
`org.apache.jackrabbit.oak.spi.security.authorization.restriction` and provide 
support for pluggable restrictions both for access control management and the 
repository internal permission evaluation:

- [RestrictionProvider]: interface to obtain restriction information needed for access control and permission management
- [Restriction]: the restriction object as created using Jackrabbit access control API
- [RestrictionDefinition]: the static definition of a supported restriction
- [RestrictionPattern]: the processed restriction ready for permission evaluation
- [AggregationAware]: optional extension to make a [RestrictionProvider] aware of being used in an composite setup, since Oak 1.44.0 (see [OAK-9782])

<a name="default_implementation"></a>
### Default Implementation

Oak 1.0 provides the following base implementations:

- `AbstractRestrictionProvider`: abstract base implementation of the provider interface. Since 1.44.0 also implements [AggregationAware] (see [OAK-9782])
- `RestrictionDefinitionImpl`: default implementation of the `RestrictionDefinition` interface.
- `RestrictionImpl`: default implementation of the `Restriction` interface.
- `CompositeRestrictionProvider`: Allows aggregating multiple provider implementations (see [Pluggability](#pluggability) below).
- `CompositePattern`: Allows aggregating multiple restriction patterns.

#### Changes wrt Jackrabbit 2.x

Apart from the fact that the internal Jackrabbit extension has been replaced by
a public API, the restriction implementation in Oak differs from Jackrabbit 2.x
as follows:

- Separate restriction management API (see below) on the OAK level that simplifies plugging custom restrictions.
- Changed node type definition for storing restrictions in the default implementation.
    - as of OAK restrictions are collected underneath a separate child node "rep:restrictions"
    - backwards compatible behavior for restrictions stored underneath the ACE node directly
- Support for multi-valued restrictions (see [JCR-3637](https://issues.apache.org/jira/browse/JCR-3637), [JCR-3641](https://issues.apache.org/jira/browse/JCR-3641))
- Validation of the restrictions is delegated to a dedicated commit hook
- Restriction `rep:glob` limits the number of wildcard characters to 20
- New restrictions `rep:ntNames`, `rep:prefixes`, `rep:itemNames` and `rep:current`
   
#### Built-in Restrictions

The following `Restriction` implementations are supported with the default Oak access control management:

| Restriction Name | Type   | Multi-Valued | Mandatory | Description                          | Since |
|------------------|--------|--------------|-----------|--------------------------------------|-------|
| `rep:glob`       | String | false        | false     | Single name, path or path pattern with '*' wildcard(s), see below for details | Oak 1.0 |
| `rep:ntNames`    | Name   | true         | false     | Multivalued restriction to limit the effect to nodes of the specified primary node types (no nt inheritance) | Oak 1.0 |
| `rep:prefixes`   | String | true         | false     | Multivalued restriction to limit the effect to item names that match the specified namespace prefixes (session level remapping not respected) | Oak 1.0 |
| `rep:itemNames`  | Name   | true         | false     | Multivalued restriction for property or node names | Oak 1.3.8 |
| `rep:current`    | String | true         | false     | Multivalued restriction that limits the effect to a single level i.e. the target node where the access control entry takes effect and optionally all or a subset of it's properties. There is no inheritance of the ACE effect to nodes in the subtree or their properties. Expanded JCR property names and namespace remapping not supported (see below for details) | Oak 1.42.0 |
| `rep:globs`      | String | true         | false     | Multivalued variant of the `rep:glob` restriction | Oak 1.44.0 |
| `rep:subtrees`   | String | true         | false     | Multivalued restriction that limits effect to one or multiple subtrees (see below) | Oak 1.44.0 |


<a name="rep_glob"></a>
##### rep:glob - Details and Examples

For a nodePath `/foo` the following results can be expected for the different
values of `rep:glob`.

Please note that the pattern is based on simple path concatenation and equally applies to either type of item (both nodes and properties). 
Consequently, the examples below need to be adjusted for the root node in order to produce the desired effect. In particular a path with two subsequent / is invalid and will never match any target item or path.

| rep:glob      | Result                                                   |
|---------------|----------------------------------------------------------|
| null          | i.e. no restriction: matches /foo and all descendants    |
| ""            | matches node /foo only (no descendants, not even properties) |

Examples including wildcard char:

| rep:glob      | Result                                                   |
|---------------|----------------------------------------------------------|
| \*            | foo, siblings of foo and their descendants               |
| /\*cat        | all descendants of /foo whose paths end with 'cat'       |
| \*cat         | all siblings and descendants of foo that have a name ending with 'cat' |
| /\*/cat       | all non-direct descendants of /foo named 'cat'           |
| /cat\*        | all descendant of /foo that have the direct foo-descendant segment starting with 'cat' |
| \*/cat        | all descendants of /foo and foo's siblings that have a name segment 'cat' |
| cat/\*        | all descendants of '/foocat'                             |
| /cat/\*       | all descendants of '/foo/cat'                            |
| /\*cat/\*     | all descendants of /foo that have an intermediate segment ending with 'cat' |

Examples without wildcard char:

| rep:glob      | Result                                                   |
|---------------|----------------------------------------------------------|
| /cat          | '/foo/cat' and all it's descendants                      |
| /cat/         | all descendants of '/foo/cat'                            |
| cat           | '/foocat' and all it's descendants                       |
| cat/          | all descendants of '/foocat'                             |

See also [GlobPattern] for implementation details and the [GlobRestrictionTest] in the _oak-exercise_ module for training material.

<a name="rep_current"></a>
##### rep:current - Details and Examples

The restriction limits the effect to the target node. The value of the restriction property defines which properties of 
the target node will in addition also match the restriction:

- an empty value array will make the restriction matching the target node only (i.e. equivalent to rep:glob=""). 
- an array of qualified property names will extend the effect of the restriction to properties of the target node that match the specified names. 
- the residual name * will match all properties of the target node.

For a nodePath `/foo` the following results can be expected for the different values of `rep:current`:

| rep:current       | Result                                                        |
|-------------------|---------------------------------------------------------------|
| []                | `/foo` only, none of it's properties                          |
| [\*]              | `/foo` and all it's properties                                |
| [jcr:primaryType] | `/foo` and it's `jcr:primaryType` property. no other property |
| [a, b, c]         | `/foo` and it's properties `a`,`b`,`c`                        |

###### Known Limitations
- Due to the support of the residual name `*`, which isn't a valid JCR name, the restriction `rep:current` is defined to be 
of `PropertyType.STRING` instead of `PropertyType.NAME`. Like the `rep:glob` restriction it will therefore not work with 
expanded JCR names or with remapped namespace prefixes.
- In case of permission evaluation for a path pointing to a *non-existing* JCR item (see e.g. `javax.jcr.Session#hasPermission(String, String)`),
it's not possible to distinguish between nodes and properties. A best-effort approach is take to identify known properties 
like e.g. `jcr:primaryType`. For all other paths the implementation of the `rep:current` restrictions assumes that they 
point to non-existing nodes. Example: if `rep:current` is present with an ACE taking effect at `/foo` the call 
`Session.hasPermission("/foo/non-existing",Session.ACTION_READ)` will always return `false` because the restriction
will interpret `/foo/non-existing` as a path pointing to a node.

<a name="rep_subtrees"></a>
##### rep:subtrees - Details and Examples

The `rep:subtrees` restriction allows to limit the effect to one or multiple subtrees below the access-controlled 
node. It is a simplified variant of the common pattern using 2 `rep:glob` (wildcard) patterns to grant or 
deny access on a particular node in the subtree and all its descendent items.

A common pattern involving `rep:glob` may look as follows:
- entry 1: `rep:glob` = "/\*/path/to/subtree"
- entry 2: `rep:glob`= "/\*/path/to/subtree/\*"

The `rep:subtrees` restriction neither requires multiple entries nor wildcard characters 
that have shown to be prone to mistakes. The corresponding setup with `rep:subtrees` would be
- entry: `rep:subtrees` = "/path/to/subtree"

and matches both the tree defined at /foo/path/to/subtree as well as any subtree at /foo/*/path/to/subtree.

For a node path `/foo` the following results can be expected for different values of `rep:subtrees`:

| `rep:subtrees`| Result |
|---------------|-------------------------------------------------------------|
| [/cat]        |   all descendants of `/foo` whose path ends with `/cat` or that have an intermediate segment `/cat/` |
| [/cat/]       |   all descendants of `/foo` that have an intermediate segment `/cat/` |
| [cat]         |   all siblings or descendants of `/foo` whose path ends with `cat` or that have an intermediate segment ending with `cat` |
| [cat/]        |   all siblings or descendants of `/foo` that have an intermediate segment ending with `cat` |

Note:
- variants of 'cat'-paths could also consist of multiple segments like e.g. `/cat/dog` or `/cat/dog`
- different subtrees can be matched by defining multiple values in the restriction
- in contrast to `rep:glob` no wildcard characters are required to make sure the restriction is evaluated against the whole tree below the access controlled node.
- an empty value array will never match any path/item
- null values and empty string values will be ignored


<a name="representation"></a>
### Representation in the Repository

All restrictions defined by default in an Oak repository are stored as properties 
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


<a name="pluggability"></a>
### Pluggability

The default security setup as present with Oak 1.0 is able to provide custom
`RestrictionProvider` implementations and will automatically combine the
different implementations using the `CompositeRestrictionProvider`.

In an OSGi setup the following steps are required in order to add an action provider
implementation:

- implement `RestrictionProvider` interface exposing your custom restriction(s).
- make the provider implementation an OSGi service and make it available to the Oak repository.
- make sure the `RestrictionProvider` is listed as required service with the `SecurityProvider` (see also [Introduction](../introduction.html#configuration]))

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
    Map<String, RestrictionProvider> authorizMap = Map.of(PARAM_RESTRICTION_PROVIDER, rProvider);
    ConfigurationParameters config =  ConfigurationParameters.of(Map.of(AuthorizationConfiguration.NAME, ConfigurationParameters.of(authorizMap)));
    SecurityProvider securityProvider = SecurityProviderBuilder.newBuilder().with(config).build();
    Repository repo = new Jcr(new Oak()).with(securityProvider).createRepository();

<!-- hidden references -->
[GlobPattern]: https://github.com/apache/jackrabbit-oak/tree/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/security/authorization/restriction/GlobPattern.java?view=markup
[GlobRestrictionTest]: https://github.com/apache/jackrabbit-oak/tree/trunk/oak-exercise/src/test/java/org/apache/jackrabbit/oak/exercise/security/authorization/accesscontrol/L8_GlobRestrictionTest.java?view=markup
[Restriction]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/restriction/Restriction.html
[RestrictionDefinition]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/restriction/RestrictionDefinition.html
[RestrictionPattern]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/restriction/RestrictionPattern.html
[RestrictionProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/restriction/RestrictionProvider.html
[AggregationAware]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/restriction/AggregationAware.html
[OAK-5784]: https://issues.apache.org/jira/browse/OAK-5784
[OAK-9782]: https://issues.apache.org/jira/browse/OAK-9782
