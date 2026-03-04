# Redundant Dependencies Analysis Report

Generated: 2026-03-04

## Executive Summary

Total modules analyzed: 46
Unique dependencies: 272

## 1. Dependencies with Multiple Versions

These dependencies are declared with different versions across modules,
which can lead to classpath conflicts and unpredictable behavior.

### com.google.guava:guava
Versions found: ${shaded.guava.version}, 33.1.0-jre

Locations:
- oak-segment-azure: 33.1.0-jre
- oak-shaded-guava: ${shaded.guava.version}

### com.googlecode.json-simple:json-simple
Versions found: 1.1, 1.1.1

Locations:
- oak-core: 1.1.1
- oak-webapp: 1.1
- oak-it: 1.1.1
- oak-jcr: 1.1.1
- oak-pojosr: 1.1.1
- oak-run: 1.1.1
- oak-store-document: 1.1.1

### commons-logging:commons-logging
Versions found: 1.2, 1.3.4

Locations:
- oak-it-osgi: 1.2
- oak-run: 1.3.4

### org.apache.felix:org.apache.felix.configadmin
Versions found: 1.8.16, 1.8.8

Locations:
- oak-pojosr: 1.8.16
- oak-run-commons: 1.8.8
- oak-run: 1.8.8

### org.apache.lucene:lucene-core
Versions found: ${lucene.version}, 3.6.0

Locations:
- oak-standalone: ${lucene.version}
- oak-pojosr: ${lucene.version}
- oak-upgrade: 3.6.0

### org.hamcrest:hamcrest-core
Versions found: 1.3, 2.2

Locations:
- oak-blob-plugins: 1.3
- oak-search-elastic: 2.2

## 2. Frequently Declared Dependencies

These dependencies are declared in many modules. Consider adding them to
dependencyManagement in oak-parent to centralize version control.

### org.apache.jackrabbit:oak-commons
Used in 38 modules

Modules: oak-auth-external, oak-auth-ldap, oak-benchmarks, oak-blob, oak-blob
... and 33 more

### org.apache.jackrabbit:oak-core
Used in 36 modules

Modules: oak-auth-external, oak-auth-external, oak-auth-ldap, oak-auth-ldap, oak-authorization-cug
... and 31 more

### org.apache.jackrabbit:oak-store-spi
Used in 28 modules

Modules: oak-auth-external, oak-authorization-cug, oak-authorization-principalbased, oak-benchmarks, oak-blob-cloud
... and 23 more

### org.apache.jackrabbit:oak-api
Used in 23 modules

Modules: oak-auth-external, oak-auth-ldap, oak-authorization-cug, oak-authorization-principalbased, oak-benchmarks
... and 18 more

### org.apache.jackrabbit:oak-jcr
Used in 20 modules

Modules: oak-auth-external, oak-authorization-cug, oak-authorization-cug, oak-authorization-principalbased, oak-exercise
... and 15 more

### org.apache.jackrabbit:oak-segment-tar
Used in 19 modules

Modules: oak-exercise, oak-it, oak-it, oak-it-osgi, oak-jcr
... and 14 more

### org.apache.jackrabbit:oak-store-document
Used in 18 modules

Modules: oak-exercise, oak-it, oak-it, oak-it-osgi, oak-jcr
... and 13 more

### org.apache.jackrabbit:oak-core-spi
Used in 17 modules

Modules: oak-benchmarks, oak-blob-cloud, oak-blob-plugins, oak-core, oak-core
... and 12 more

### org.apache.jackrabbit:jackrabbit-jcr-commons
Used in 16 modules

Modules: oak-auth-external, oak-authorization-cug, oak-authorization-principalbased, oak-blob-cloud, oak-blob-cloud-azure
... and 11 more

### org.apache.jackrabbit:oak-blob-plugins
Used in 16 modules

Modules: oak-blob-cloud, oak-blob-cloud, oak-blob-cloud-azure, oak-blob-cloud-azure, oak-core
... and 11 more

### org.apache.jackrabbit:oak-query-spi
Used in 15 modules

Modules: oak-auth-external, oak-authorization-cug, oak-authorization-principalbased, oak-benchmarks, oak-core
... and 10 more

### org.apache.jackrabbit:oak-blob-cloud-azure
Used in 13 modules

Modules: oak-benchmarks, oak-it, oak-it, oak-it-osgi, oak-jcr
... and 8 more

### org.apache.jackrabbit:oak-jackrabbit-api
Used in 11 modules

Modules: oak-auth-external, oak-authorization-cug, oak-authorization-principalbased, oak-blob-plugins, oak-core
... and 6 more

### org.apache.jackrabbit:oak-security-spi
Used in 11 modules

Modules: oak-auth-external, oak-auth-ldap, oak-authorization-cug, oak-authorization-principalbased, oak-benchmarks
... and 6 more

### org.apache.jackrabbit:oak-store-composite
Used in 11 modules

Modules: oak-authorization-cug, oak-authorization-principalbased, oak-exercise, oak-it, oak-it-osgi
... and 6 more

### org.apache.jackrabbit:oak-blob
Used in 11 modules

Modules: oak-blob-cloud, oak-blob-cloud, oak-blob-cloud-azure, oak-blob-cloud-azure, oak-blob-plugins
... and 6 more

### com.h2database:h2
Used in 10 modules

Modules: oak-doc-railroad-macro, oak-it, oak-jcr, oak-lucene, oak-pojosr
... and 5 more

### org.apache.jackrabbit:oak-blob-cloud
Used in 9 modules

Modules: oak-benchmarks, oak-it, oak-it, oak-jcr, oak-run
... and 4 more

### org.apache.jackrabbit:oak-shaded-guava
Used in 9 modules

Modules: oak-blob, oak-blob-cloud, oak-blob-cloud-azure, oak-blob-plugins, oak-commons
... and 4 more

### org.hamcrest:hamcrest-all
Used in 9 modules

Modules: oak-core, oak-core-spi, oak-lucene, oak-run, oak-run-commons
... and 4 more

### com.googlecode.json-simple:json-simple
Used in 7 modules

Modules: oak-core, oak-it, oak-jcr, oak-pojosr, oak-run
... and 2 more

### org.apache.jackrabbit:oak-segment-azure
Used in 7 modules

Modules: oak-it, oak-it, oak-it-osgi, oak-jcr, oak-jcr
... and 2 more

### org.apache.jackrabbit:oak-lucene
Used in 6 modules

Modules: oak-benchmarks-lucene, oak-it-osgi, oak-pojosr, oak-run, oak-standalone
... and 1 more

### org.apache.jackrabbit:oak-search
Used in 6 modules

Modules: oak-benchmarks, oak-lucene, oak-lucene, oak-run-commons, oak-search-elastic
... and 1 more

### org.apache.jackrabbit:jackrabbit-data
Used in 6 modules

Modules: oak-blob, oak-blob-cloud, oak-blob-cloud-azure, oak-blob-cloud-azure, oak-blob-plugins
... and 1 more

### org.apache.tika:tika-parsers
Used in 6 modules

Modules: oak-lucene, oak-pojosr, oak-run, oak-search-elastic, oak-standalone
... and 1 more

### org.apache.tika:tika-core
Used in 6 modules

Modules: oak-http, oak-lucene, oak-pojosr, oak-run, oak-run-elastic
... and 1 more

### org.apache.jackrabbit:oak-auth-external
Used in 5 modules

Modules: oak-auth-ldap, oak-auth-ldap, oak-benchmarks, oak-exercise, oak-it-osgi

### org.apache.jackrabbit:oak-run-commons
Used in 5 modules

Modules: oak-benchmarks, oak-run, oak-run, oak-run-elastic, oak-run-elastic

### org.apache.jackrabbit:oak-segment-remote
Used in 5 modules

Modules: oak-it-osgi, oak-run-commons, oak-segment-aws, oak-segment-azure, oak-upgrade

### org.apache.jackrabbit:oak-segment-aws
Used in 5 modules

Modules: oak-it, oak-it, oak-jcr, oak-jcr, oak-run-commons

### org.apache.jackrabbit:jackrabbit-core
Used in 5 modules

Modules: oak-jcr, oak-jcr, oak-lucene, oak-run-commons, oak-upgrade

## 3. Redundant Version Declarations

These dependencies specify a version explicitly even though the version
is already managed in oak-parent dependencyManagement. The version
declaration can be removed to rely on the parent version.

- oak-api: javax.jcr:jcr (specifies version 2.0)
- oak-auth-external: org.apache.sling:org.apache.sling.testing.osgi-mock.core (specifies version 3.4.2)
- oak-auth-external: org.apache.sling:org.apache.sling.testing.osgi-mock.junit4 (specifies version 3.4.2)
- oak-auth-ldap: javax.jcr:jcr (specifies version 2.0)
- oak-authorization-cug: javax.jcr:jcr (specifies version 2.0)
- oak-authorization-principalbased: javax.jcr:jcr (specifies version 2.0)
- oak-benchmarks: javax.jcr:jcr (specifies version 2.0)
- oak-benchmarks: org.apache.sling:org.apache.sling.testing.osgi-mock.core (specifies version 3.4.2)
- oak-blob: javax.jcr:jcr (specifies version 2.0)
- oak-blob-cloud: javax.jcr:jcr (specifies version 2.0)
- oak-blob-cloud: org.reactivestreams:reactive-streams (specifies version 1.0.4)
- oak-blob-plugins: javax.jcr:jcr (specifies version 2.0)
- oak-core: javax.jcr:jcr (specifies version 2.0)
- oak-core-spi: javax.jcr:jcr (specifies version 2.0)
- oak-exercise: javax.jcr:jcr (specifies version 2.0)
- oak-jackrabbit-api: javax.jcr:jcr (specifies version 2.0)
- oak-jackrabbit-api: org.osgi:org.osgi.annotation.versioning (specifies version 1.0.0)
- oak-jcr: javax.jcr:jcr (specifies version 2.0)
- oak-lucene: com.github.stefanbirkner:system-rules (specifies version 1.19.0)
- oak-pojosr: org.osgi:osgi.core (specifies version 6.0.0)
- oak-run: com.github.stefanbirkner:system-rules (specifies version 1.19.0)
- oak-run: org.apache.commons:commons-csv (specifies version 1.1)
- oak-run-commons: com.github.stefanbirkner:system-rules (specifies version 1.19.0)
- oak-search-elastic: com.github.stefanbirkner:system-rules (specifies version 1.19.0)
- oak-security-spi: javax.jcr:jcr (specifies version 2.0)
- oak-segment-azure: com.azure:azure-storage-blob (specifies version 12.25.3)
- oak-segment-azure: com.azure:azure-storage-common (specifies version 12.24.3)
- oak-segment-azure: com.azure:azure-xml (specifies version 1.0.0)
- oak-segment-azure: com.google.code.findbugs:jsr305 (specifies version 3.0.2)
- oak-segment-tar: javax.jcr:jcr (specifies version 2.0)
- oak-standalone: javax.jcr:jcr (specifies version 2.0)
- oak-standalone: org.mongodb:mongodb-driver-sync (specifies version 4.6.1)
- oak-store-document: javax.jcr:jcr (specifies version 2.0)
- oak-store-spi: javax.jcr:jcr (specifies version 2.0)
- oak-webapp: javax.jcr:jcr (specifies version 2.0)

## 4. Test Dependencies Analysis

Common test dependencies used across modules:

- junit:junit: used in 36 modules
- org.mockito:mockito-core: used in 35 modules
- ch.qos.logback:logback-classic: used in 24 modules
- org.apache.jackrabbit:oak-commons: used in 21 modules
- org.apache.jackrabbit:oak-core: used in 20 modules
- org.apache.sling:org.apache.sling.testing.osgi-mock.junit4: used in 16 modules
- org.osgi:org.osgi.service.event: used in 16 modules
- org.osgi:org.osgi.service.log: used in 16 modules
- org.slf4j:jul-to-slf4j: used in 14 modules
- org.osgi:org.osgi.service.cm: used in 14 modules
- org.apache.jackrabbit:oak-jcr: used in 13 modules
- org.apache.jackrabbit:oak-store-document: used in 13 modules
- org.apache.jackrabbit:oak-blob-plugins: used in 11 modules
- org.apache.jackrabbit:oak-segment-tar: used in 11 modules
- org.testcontainers:testcontainers: used in 10 modules
- io.dropwizard.metrics:metrics-core: used in 10 modules
- org.hamcrest:hamcrest-all: used in 9 modules
- org.apache.jackrabbit:oak-store-composite: used in 8 modules
- org.apache.jackrabbit:oak-store-spi: used in 8 modules
- org.apache.jackrabbit:oak-blob-cloud-azure: used in 8 modules
- org.apache.jackrabbit:oak-blob: used in 6 modules
- org.osgi:org.osgi.service.component: used in 5 modules
- org.apache.jackrabbit:oak-segment-azure: used in 5 modules
- com.h2database:h2: used in 5 modules
- com.github.stefanbirkner:system-rules: used in 5 modules

## Recommendations

1. **Version Conflicts**: Review dependencies with multiple versions and standardize
   on a single version across all modules. Add to parent dependencyManagement.

2. **Centralize Version Management**: Move frequently used dependencies to
   oak-parent dependencyManagement to ensure consistent versions.

3. **Remove Redundant Versions**: Remove explicit version declarations from
   module pom.xml files when the version is already managed in parent.

4. **Test Dependencies**: Consider standardizing test dependency versions
   in parent dependencyManagement for consistency.
