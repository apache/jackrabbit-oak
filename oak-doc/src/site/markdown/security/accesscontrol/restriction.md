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

_todo_

### Restriction API

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

_todo_

#### Built-in Restriction Implementations

The default implementations of the `Restriction` interface are present with
Oak 1.0 access control management:

* `rep:glob`:
* `rep:ntNames`:
* `rep:prefixes`:


### Pluggability

The default security setup as present with Oak 1.0 is able to provide custom
`RestrictionProvider` implementations and will automatically combine the
different implementations using the `CompositeRestrictionProvider`.

In an OSGi setup the following steps are required in order to add a action provider
implementation:

- implement `RestrictionProvider` interface exposing your custom restriction(s).
- make the provider implementation an OSGi service and make it available to the Oak repository.

#### Examples

##### Example RestrictionProvider

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

##### Example RestrictionPattern

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


<!-- hidden references -->
[Restriction]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/restriction/Restriction.html
[RestrictionDefinition]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/restriction/RestrictionDefinition.html
[RestrictionPattern]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/restriction/RestrictionPattern.html
[RestrictionProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/restriction/RestrictionProvider.html
