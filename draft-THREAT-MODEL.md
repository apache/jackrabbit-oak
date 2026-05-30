<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Jackrabbit Oak Security Threat Model (draft)

**Why a separate Jackrabbit-Oak model (not a single Jackrabbit-PMC umbrella).**
The Jackrabbit PMC owns three functionally distinct codebases that share a
common JCR API contract but have completely different security architectures:
the original `jackrabbit` (jackrabbit-core, JR2-era), `jackrabbit-oak` (the
modern, scalable successor with a different storage model and a redesigned
security stack), and `jackrabbit-filevault` (a packaging / serialisation
tool whose entire reason for existing is to move repository content across a
trust boundary as a zip file). An umbrella model would have to disclaim each
of the per-repo nuances in turn — every "the project trusts X" statement
would carry "...for Oak, but jackrabbit-core uses a different mechanism, and
filevault doesn't have callers in this sense". Three smaller models cite
each other for the JCR contract and stand on their own for everything else.
The triage utility of a closed-set §13 disposition table requires that
each project's set actually be closed.

## §1 Header

- **Project:** Apache Jackrabbit Oak (`apache/jackrabbit-oak`) *(documented:
  `AGENTS.md`, `.asf.yaml`)*. Oak is the modern JCR repository
  implementation; the original `jackrabbit` (jackrabbit-core) is modelled
  separately, and `jackrabbit-filevault` (packaging) is also modelled
  separately.
- **Commit / version binding:** drafted against the default branch
  (`trunk`) *(documented: `.asf.yaml` — `protected_branches: trunk`)*. A
  vulnerability report against Oak version *N* should be triaged against
  the model as it stood at *N* (release tag), not against `trunk`.
- **Date:** 2026-05-30.
- **Authors:** ASF Security team draft, awaiting Jackrabbit PMC review.
- **Status:** draft — under maintainer review.
- **Reporting cross-reference:** findings that may violate a §8 property
  should be reported per the ASF Security Team disclosure channel
  (`security@apache.org`) and the Jackrabbit project's security mailing
  list, before public disclosure *(documented: Oak's `security/reports.md`
  — "The Apache Security team requests that researchers report
  undisclosed vulnerabilities to the security mailing list before public
  disclosure")*. Findings that fall under §3, §9, or §11a will be
  closed by Oak triagers citing this document.
- **Provenance legend:**
  *(documented)* — drawn from in-repo docs or website docs with citation;
  *(maintainer)* — confirmed by an Oak maintainer in response to this
  draft; *(inferred)* — synthesised from code structure or domain
  knowledge, awaiting PMC ratification (every *(inferred)* tag has a
  matching §14 question).
- **Draft confidence:** 35 documented / 0 maintainer / 28 inferred.

**About the project.** Apache Jackrabbit Oak is the actively-developed
scalable hierarchical content repository that succeeded the original
Apache Jackrabbit (jackrabbit-core). It implements the JCR 2.0
specification (JSR 283) and is the storage engine that ships with Adobe
Experience Manager and several other CMS / DAM products. The
implementation is split across ~47 Maven modules under
`oak-*`; it is intended to be **embedded** by a host application (CMS,
asset manager, integration framework, …) — not deployed as a standalone
server — and exposes a JCR `Repository` API and an Oak `ContentRepository`
API. Security is structured around a pluggable `SecurityProvider` that
binds an `AuthenticationConfiguration`, an `AuthorizationConfiguration`
(possibly composite), a `UserConfiguration`, a `PrincipalConfiguration`,
and a `PrivilegeConfiguration` *(documented: `security/introduction.md`,
`security/overview.md`)*.

## §2 Scope and intended use

### Intended use

- **In-process JCR 2.0 (JSR 283) repository** embedded by a host
  application (CMS, DAM, integration framework). The Oak repository
  exposes `javax.jcr.Repository` and `org.apache.jackrabbit.oak.api.ContentRepository`
  to in-process callers; there is no built-in network listener and no
  per-end-user authn/authz outside what the host plugs into the
  `LoginContextProvider` and `AuthorizationConfiguration` chain
  *(documented: `security/introduction.md`,
  `security/authentication.md`)*.
- The repository supports multiple NodeStore backends — DocumentNodeStore
  (Mongo, RDB), SegmentNodeStore (TarMK), Composite, with Blob storage
  in BlobStore (S3, Azure, FileBlobStore, …) *(documented: `AGENTS.md` —
  "Persistence: Multiple NodeStore backends (Document, Segment/TarMK,
  Composite, AWS S3, Azure)")*.
- Indexing is via Lucene (oak-lucene) and Elasticsearch (oak-search-elastic)
  *(documented: `AGENTS.md`)*.

### Deployment shape

Oak is **not** a standalone daemon and is **not** a network service in its
own right. It is an in-process library. Network exposure (HTTP, WebDAV,
custom protocol) is **always** an artefact of the host application; Oak
ships no listener of its own *(inferred — §14 Q1)*. The threat model
is therefore that of a library, not a service — but a library whose
contract specifically promises authentication and authorisation
properties to its host, which makes it a more security-load-bearing
library than (say) zlib.

### Caller roles

Following §2 of the output-structure rubric (in-process-library split):

| Role | Trust level | Notes |
| --- | --- | --- |
| **Host application code** | trusted | Holds the `Repository`/`ContentRepository` handle; chooses the `SecurityProvider` and `LoginContextProvider`; configures the NodeStore + BlobStore; may bypass authorisation by obtaining a system-level session via `loginAdministrative` / `loginService` *(documented: `security/permission/default.md` — admin/system principals bypass permission evaluation)*. The host decides whether end-user credentials reach Oak at all. |
| **JCR session principal (end user)** | untrusted but authenticated | Identifies through `Repository.login(Credentials)` or `ContentRepository.login(Credentials, workspaceName)`; subjected to the configured `PermissionProvider` chain on every read/write. The principal is authenticated by Oak's `LoginContext` *(documented: `security/authentication.md`)*. |
| **System / admin principal** | trusted | A session obtained via `loginAdministrative` / `loginService` (host-driven) carries `SystemPrincipal` or `AdminPrincipal` and bypasses permission evaluation *(documented: `security/permission/default.md` — "Three principal categories automatically receive full repository access: SystemPrincipal, AdminPrincipal, and Principals matching configured administrative names")*. |
| **External identity provider** | trusted control plane | The host configures one or more `ExternalIdentityProvider`s for LDAP / SAML / OAuth; Oak's `ExternalLoginModule` accepts whatever identity these IDPs assert *(documented: `security/authentication/externalloginmodule.md` — "The mechanism implicitly trusts that the configured IDP accurately authenticates identities")*. |
| **Pre-authenticated caller** | trusted (operator-asserted) | When `PreAuthenticatedLogin` is in use, Oak performs no credential verification at all; the host is asserting "this user has already been authenticated upstream" *(documented: `security/authentication/preauthentication.md` — "Oak delegates all authentication responsibility to the caller")*. |
| **NodeStore backend** | trusted | Mongo / Tar / Segment / RDB / Composite storage is assumed honest and assumed to enforce its own at-rest protections *(inferred — §14 Q2)*. |
| **BlobStore backend** | trusted | S3 / Azure / FileBlobStore is assumed honest *(inferred — §14 Q2)*. |

### Component-family table

| Family | Representative entry | Touches outside the process? | In-model? |
| --- | --- | --- | --- |
| `oak-api`, `oak-core`, `oak-core-spi` — content tree, MVCC, commit hooks | `ContentRepository.login` | no (only through a NodeStore) | **yes** |
| `oak-jcr` — JCR 2.0 binding *(documented: `AGENTS.md`)* | `Repository.login` | no | **yes** |
| `oak-security-spi`, default `AuthorizationConfiguration`, default `PermissionProvider` | `SecurityProvider` | no | **yes** (high security weight; 100% test coverage mandate per `AGENTS.md`) |
| `oak-authorization-cug` — Closed User Groups *(documented: `security/authorization/cug.md`)* | composite `AuthorizationConfiguration` | no | **yes** (read-only authorisation only — see §9.10) |
| `oak-authorization-principalbased` — principal-based authz *(documented: `AGENTS.md`)* | composite `AuthorizationConfiguration` | no | **yes** |
| `oak-auth-external` — IDP framework *(documented: `security/authentication/externalloginmodule.md`)* | `ExternalIdentityProvider` SPI | depends on IDP impl | **yes** for the wrapper; IDP impl is per-host |
| `oak-auth-ldap` — LDAP IDP *(documented: `AGENTS.md`)* | `LdapIdentityProvider` | **yes — LDAP/AD** | **yes** |
| Persistence — `oak-store-document` (Mongo / RDB), `oak-segment-tar`, `oak-store-composite`, `oak-store-spi` | `DocumentNodeStore`, `SegmentNodeStore` | **yes — DB / FS** | **yes** for in-Oak code; backend itself is trusted (§3) |
| BlobStore — `oak-blob`, `oak-blob-cloud`, `oak-blob-cloud-azure`, `oak-blob-plugins` | `S3DataStore`, `AzureDataStore` | **yes — S3 / Azure / FS** | **yes** for in-Oak code; cloud APIs trusted (§3) |
| Search — `oak-lucene`, `oak-search`, `oak-search-elastic` | `IndexEditor`, query parsers | sometimes (Elasticsearch over HTTP) | **yes** |
| `oak-run` — operator CLI / tooling | `oak-run.jar` | OS / FS / network depending on subcommand | **see §3** (in-model only for the command-driven contract; out-of-model for "operator runs it as the wrong user") |
| `oak-pojosr`, `oak-standalone` — repository launchers | embedded repository | filesystem | **yes** for code; deployment is operator's |
| `oak-upgrade` — JR2 → Oak migration | offline migration job | filesystem | **yes** for code; the migration source is a trusted JR2 repository |
| `oak-it`, `oak-it-osgi`, `oak-bench-*`, `oak-jcr-tests`, `oak-test-bundle`, `oak-exercise` | integration tests, benchmarks, training | varies | **out of model** — unsupported components *(§3)* |
| `oak-examples`, `oak-doc-railroad-macro`, `oak-doc` | examples and docs | none | **out of model** *(§3)* |
| Archived MicroKernel modules (`oak-mk-*`) | n/a | n/a | **out of model** — explicitly archived *(documented: README — "MicroKernel-related modules have been archived")* |

A finding is in-model only if it lands in a row marked **yes**. See §4
for per-component reachability tests.

## §3 Out of scope (explicit non-goals)

Reports requiring any of these will be closed with the cited disposition:

1. **Host application correctness.** Oak is embedded. If the host hands
   out an admin session to an unauthenticated HTTP request, exposes
   `loginAdministrative` over JMX, or routes user-supplied SQL2 queries
   directly into the session without filtering, the harm is the host's
   *(documented: `security/permission/default.md` — admin/system
   principals bypass permission evaluation)*. → `OUT-OF-MODEL:
   adversary-not-in-scope`.
2. **NodeStore / BlobStore / IDP correctness.** Mongo, RDB, TarMK on
   disk, S3, Azure, LDAP, SAML — Oak trusts the responses these systems
   give. A backend returning forged bytes, an LDAP server asserting a
   spoofed group membership, an S3 bucket allowing unauthorised reads —
   none are Oak vulnerabilities *(inferred — §14 Q2)*. →
   `OUT-OF-MODEL: trusted-input`.
3. **Storage-level authorisation.** HDFS / S3 / filesystem ACLs on the
   underlying NodeStore / BlobStore are the operator's responsibility. A
   tar-store file readable by `other` is not an Oak bug *(inferred —
   §14 Q3)*. → `OUT-OF-MODEL: adversary-not-in-scope`.
4. **Pre-authentication misuse.** The `PreAuthenticatedLogin` mechanism
   is an *explicit* bypass: Oak does no credential verification at all
   and trusts that an upstream layer has *(documented:
   `security/authentication/preauthentication.md`)*. A report that the
   pre-auth code path "trusts the caller" is a documented design
   choice. → `BY-DESIGN: property-disclaimed` (§9).
5. **Custom `SecurityProvider` / `LoginModule` replacements.** Oak
   ships a default but documents that custom implementations are "only
   recommended for experts having in-depth understanding of Oak
   internals and which understand the security risk associated with
   custom replacements" *(documented: `security/introduction.md`)*. A
   report that requires a custom SPI implementation that voids a
   guarantee is the host's choice. → `OUT-OF-MODEL: non-default-build`.
6. **`oak-run` invoked by the operator.** `oak-run` is an offline
   administrative CLI; running it requires direct filesystem and
   credential access. "Operator runs `oak-run console` and dumps the
   repository" is not a vulnerability *(inferred — §14 Q4)*. →
   `OUT-OF-MODEL: adversary-not-in-scope`.
7. **Code that ships but is not part of the supported product:**
   `oak-it/`, `oak-it-osgi/`, `oak-bench-*/`, `oak-jcr-tests/`,
   `oak-test-bundle/`, `oak-exercise/`, `oak-examples/`,
   `oak-doc-railroad-macro/`, archived `oak-mk-*` modules
   *(documented: README, AGENTS.md)*. → `OUT-OF-MODEL:
   unsupported-component`.
8. **Original `jackrabbit` / `jackrabbit-core` code.** Oak migrated away
   from the JR2 codebase; jackrabbit-core has a separate threat model.
   The `oak-upgrade` module imports from jackrabbit-core as a one-shot
   migration source; a bug in the JR2-side code is jackrabbit-core's
   threat-model problem. → `OUT-OF-MODEL: unsupported-component` (with
   cross-reference).
9. **`jackrabbit-filevault` package import** — filevault has its own
   threat model. A vulnerable filevault install hook is filevault's
   problem; Oak's role is to honour the JCR session privileges that
   filevault uses *(inferred — §14 Q5)*. → `OUT-OF-MODEL:
   unsupported-component` (with cross-reference).
10. **Build / release / SDLC hygiene.** Action pinning, signing,
    reproducible builds, branch protection — out of model per the SKILL.
11. **Side channels** (cache timing, branch prediction, co-tenant on the
    same JVM). *(inferred — §14 Q6)* → `OUT-OF-MODEL:
    adversary-not-in-scope`.

## §4 Trust boundaries and data flow

Oak's trust boundary is **the JCR `Session` / Oak `ContentSession` API
surface**. Once a `Session` has been obtained, every read/write goes
through the configured `PermissionProvider` chain — *unless* the
principal is `SystemPrincipal`, `AdminPrincipal`, or matches a
configured administrative name, in which case permission evaluation is
bypassed *(documented: `security/permission/default.md` — "Three
principal categories automatically receive full repository access ...
Administrator sessions bypass permission evaluation entirely")*.

There are six trust transitions a finding must land in to be in-model:

| # | Transition | Who authenticates | Who authorises |
| --- | --- | --- | --- |
| B1 | Host → `Repository.login(Credentials)` / `ContentRepository.login(Credentials, ws)` | `LoginContext` chain (`LoginModule` impls in the configured JAAS appname; default is `LoginModuleImpl` + optional `TokenLoginModule` + optional `ExternalLoginModule`) *(documented: `security/authentication.md`)* | n/a at this transition |
| B2 | Host → `loginAdministrative` / `loginService` | trusted: the host *is* the system *(documented: `security/permission/default.md`)* | n/a — these sessions bypass authorisation |
| B3 | JCR session principal → tree read | `Subject` already established at B1 | `PermissionProvider` (default + optional CUG + optional principalbased, composed) *(documented: `security/authorization.md`, `security/permission.md`)* |
| B4 | JCR session principal → tree write | same as B3 | `PermissionValidator` commit hook *(documented: `security/permission/evaluation.md`)* |
| B5 | `ExternalLoginModule` → external IDP (LDAP, SAML, …) | the IDP authenticates the user *(documented: `security/authentication/externalloginmodule.md`)* | per-IDP; Oak honours whatever group membership the IDP asserts on sync |
| B6 | Oak → NodeStore / BlobStore (Mongo, Tar, RDB, S3, Azure) | backend's own auth (operator-configured) | backend's own ACLs |

### Reachability preconditions per family

A finding is in-model only if it meets the family's reachability test:

- **`oak-api`, `oak-core`, `oak-jcr`**: reachable from a session
  established at B1 with the principal carrying *less than*
  `SystemPrincipal` / `AdminPrincipal`. A finding that requires an
  already-admin session collapses to "the host gave away an admin
  session", which is §3 item 1.
- **`oak-security-spi`, default `AuthorizationConfiguration`,
  `PermissionProvider`**: in-model for any reachable failure mode that
  results in an effective grant the configured ACLs do not license, or
  an effective deny that they do. Hidden-item handling is in-model:
  "system principals gain access except for hidden items that are not
  exposed on the Oak API" *(documented:
  `security/permission/default.md`)*.
- **`oak-authorization-cug`**: in-model only for *read* access
  restriction; CUG "solely evaluates and enforces read access to
  regular nodes and properties" *(documented:
  `security/authorization/cug.md`)*. CUG is disabled by default.
- **`oak-authorization-principalbased`**: in-model when configured;
  composes via `CompositeAuthorizationConfiguration` *(documented:
  `security/authorization.md`)*.
- **`oak-auth-external`, `oak-auth-ldap`**: in-model for the bridge
  code that *uses* the IDP — credential handling, session sync,
  group-membership materialisation into Oak's user-management tree.
  Out-of-model for the IDP's own correctness (§3 item 2).
- **NodeStore / BlobStore**: in-model for in-Oak code paths (commit
  hook, MVCC, secondary indexes); out-of-model for the backend's
  external behaviour (§3 item 2 / item 3).
- **`oak-lucene` / `oak-search-elastic`**: in-model for query
  evaluation against the *visible* tree under the caller's
  permissions. Index *leakage* — e.g. a search result that surfaces a
  node path that the caller has no read permission for — is in-model
  (the search must respect §3 §4 permission scope). The
  `QueryEngine` is documented to filter by permission as part of
  result delivery *(inferred — §14 Q7)*.
- **`oak-run`**: in-model only for in-process logic; "operator runs
  it with a bad keystore on the file system" is out (§3 item 6).

## §5 Assumptions about the environment

- **JVM / runtime.** Java 11+ at HEAD; the build requires Maven 3.x
  *(documented: `README.md`)*. JVM is conformant; the security manager
  is *not* required (modern Java has deprecated it; Oak is not designed
  to defend against in-JVM attackers).
- **Concurrency.** `Repository` is `Send`-equivalent (thread-safe in
  the JCR sense); `Session` is **not** thread-safe and is documented
  to be per-thread *(inferred — §14 Q8)*. Oak relies on MVCC for
  isolation between concurrent sessions.
- **MVCC.** Oak's content tree is an MVCC layer over a NodeStore; each
  session sees a consistent revision *(documented: `AGENTS.md` —
  "MVCC transactional implementation")*. The revision is captured at
  session login; later writes by other sessions are not visible until
  refresh.
- **Memory.** JVM-managed; Oak holds in-memory caches (node cache,
  permission cache, principal cache). Cache sizing is operator-
  configurable; pathological cache patterns (e.g. millions of distinct
  principals) can cause memory pressure but are not security failures
  *(inferred — §14 Q9)*.
- **Time.** `System.currentTimeMillis` is used for token expiry, lock
  timeouts, and TTL caches. Clock skew within the same JVM is
  irrelevant; clock skew between Oak and an external IDP (LDAP, SAML)
  is the operator's responsibility.
- **Filesystem.** SegmentNodeStore writes tar files under
  `repository.home`; DocumentNodeStore stores nothing on disk by
  default. FileBlobStore writes blob files. Permissions on these
  directories are the operator's responsibility *(inferred —
  §14 Q3)*.
- **Network.** Oak itself opens no listening sockets. Outbound
  connections are to: Mongo / RDB (DocumentNodeStore), S3 / Azure
  (BlobStore), LDAP (oak-auth-ldap), Elasticsearch
  (oak-search-elastic). All endpoints are operator-configured trusted
  *(inferred — §14 Q10)*.
- **What Oak does NOT do to its host** *(predominantly negative
  claims, awaiting maintainer ratification — §14 Q11)*:
  - opens **no** listening sockets;
  - installs **no** process-wide signal handlers;
  - does **not** spawn child processes from the core;
  - reads a documented set of system properties (e.g.
    `oak.*`) but does not consume arbitrary `LD_*` /
    `JAVA_TOOL_OPTIONS` for security-sensitive behaviour;
  - writes log entries through SLF4J (host-configured backend);
  - persists nothing outside the configured NodeStore + BlobStore
    home.

## §5a Build-time and configuration variants

Oak ships as a single feature-rich library, but the **`SecurityProvider`
composition** materially changes the security envelope. The
maintainer-confirmed list is to be ratified per §14 Q12; the
security-relevant subset:

| Knob / configuration | Default | Maintainer stance | Effect |
| --- | --- | --- | --- |
| `SecurityProvider` impl | `SecurityProviderImpl` (default) | dev/test-or-prod depending on the rest of the host config *(inferred — §14 Q12)* | every security property in §8 is *conditioned* on the default impl |
| `AuthenticationConfiguration` | `AuthenticationConfigurationImpl` (`LoginModuleImpl` + `GuestLoginModule` + `TokenLoginModule`) *(documented: `security/authentication.md`)* | configurable per JAAS appname | which `LoginModule`s are in the chain decides what credentials Oak accepts |
| `AuthorizationConfiguration` | `AuthorizationConfigurationImpl` (default ACL) | required for any meaningful authz | absent → everything is allowed for whoever logs in |
| `CompositeAuthorizationConfiguration` | not composed by default | composed in for CUG and principal-based authz | when composed, the *strictest* result across configurations applies |
| `oak-authorization-cug` | not enabled | optional; required to be composed in *(documented: `security/authorization/cug.md`)* | enables read-restriction-by-group on configured supported paths |
| `oak-authorization-principalbased` | not enabled | optional *(documented: `AGENTS.md`)* | adds principal-keyed AC evaluation |
| `oak-auth-external` + `ExternalIdentityProvider` impl | none registered | optional | enables IDP-asserted identities into the JAAS chain |
| `oak-auth-ldap` | not enabled | optional | concrete LDAP `ExternalIdentityProvider` |
| `PreAuthenticatedLogin` | not in chain by default | dev/integration | when configured, Oak skips credential verification *(documented: `security/authentication/preauthentication.md`)* — §3 item 4 |
| `TokenConfiguration` | enabled by default in default chain | configurable token expiry / refresh | misconfigured TTL extends a stolen token's usefulness |
| `UserConfiguration` password hashing | PBKDF2 (current default per `PasswordUtil`) *(documented: `security/user.md` — "PasswordUtil supports Password-Based Key Derivation Function 2 (PBKDF2)")* | maintainer ruling required per §14 Q13: are pre-PBKDF2 hash schemes still acceptable on legacy users, or must operators force a rehash? | which hash scheme guards stored passwords |
| Administrative principal whitelist | `admin` user + `SystemPrincipal` *(documented: `security/permission/default.md`)* | adding more principals here disables authz for them | "Principals matching configured administrative names" bypass permission evaluation |
| `SecureNodeBuilder` wrapping | applied to non-admin sessions *(documented: `security/permission/evaluation.md`)* | required for permission enforcement on writes | admin sessions skip `SecureNodeBuilder` entirely |
| `CompositeNodeStore` / Mount-point isolation | not by default; configurable | required for multi-mount deployments | CUG explicitly cannot be used with non-default mounts intersecting supported paths *(documented: `security/authorization/cug.md`)* |
| `Whiteboard` / `FT_OAK-<issue>` feature toggles | varies per toggle | per-toggle ruling *(documented: `AGENTS.md` — "Non-trivial changes use toggles named `FT_OAK-<issue>`")* | a security-touching toggle in the on or off position can void a §8 property |
| Index hidden-properties suppression | hidden items not exposed on Oak API *(documented: `security/permission/default.md` — "system principals gain access except for hidden items that are not exposed on the Oak API")* | structural | hidden permission-store nodes (`/jcr:system/rep:permissionStore`) are immutable through standard APIs *(documented)* |

**The insecure-default case.** Two defaults are load-bearing for triage:

- The `admin` user *exists by default* and has full repository access.
  Is "the operator failed to disable / rotate the `admin` user" a
  `VALID` Oak report or an `OUT-OF-MODEL` operator misconfiguration?
  *(maintainer ruling required — §14 Q14)*. The model assumes the
  latter: the operator must rotate `admin` per §10.
- Pre-PBKDF2 hashed passwords on legacy users — see §14 Q13.

## §6 Assumptions about inputs

### Per-entry-point trust table (in-process API)

| Entry point | Parameter | Attacker-controllable in the model? | Caller must enforce |
| --- | --- | --- | --- |
| `Repository.login(Credentials)` | `Credentials` | **yes** (this is *the* untrusted input) | configured `LoginModule`s do the work; host need not pre-validate |
| `Session.getNode(path)`, `getProperty(path)`, … | `path` | **yes** via the authenticated user, but reads filtered through `PermissionProvider` *(documented: `security/permission/evaluation.md`)* | nothing |
| `Session.save()` | accumulated transient changes | yes via the authenticated user, validated by `PermissionValidator` commit hook *(documented: `security/permission/evaluation.md`)* | nothing |
| `QueryManager.createQuery(stmt, lang)` | SQL2 / XPath text | **yes** (free-form query language) | Oak parses + plans + filters results by permissions |
| `AccessControlManager.setPolicy(path, policy)` | path + ACE list | only by callers with `MODIFY_ACCESS_CONTROL` privilege *(documented: `security/authorization.md`)* | Oak gates by privilege |
| `UserManager.createUser(id, password)` | user id, password | only by callers with the right privileges | password is hashed per `PasswordUtil` |
| `UserManager.createSystemUser(id, intermediatePath)` | id | only by callers with the right privileges | system users have no password and are intended for service identities |
| `loginAdministrative()` / `loginService(subject, ws)` | n/a | **none** — host code only; trusted | host must not expose these to user code |
| `PreAuthenticatedLogin` marker | n/a | **none** — host code only | host must not let user-supplied bytes reach the pre-auth code path |
| `ExternalIdentityProvider.authenticate(creds)` | credentials forwarded from JAAS | **yes** | the IDP impl is the trust anchor *(documented: `security/authentication/externalloginmodule.md`)* |
| `oak-run` subcommands | argv | **none** — operator-controlled | OS-level filesystem perms on the configured `repository.home` |
| Index documents (Lucene / Elasticsearch) | content of indexed properties | yes (whoever wrote the property) | query results filter by `PermissionProvider`; index *content* is treated as data |
| NodeStore byte stream | Tar blocks, Mongo BSON, RDB rows | **no** — trusted control plane | backend must not be hostile (§3 item 2) |

### Per-entry-point trust table (network-adjacent — through optional bridges)

| Surface | Parameter | Attacker-controllable? | Caller must enforce |
| --- | --- | --- | --- |
| LDAP server response (oak-auth-ldap) | DN, attributes, group membership | only via a hostile LDAP, which is §3 item 2 | TLS to the LDAP server is the operator's job |
| External IDP token / assertion | SAML response, OAuth ID token | only via a hostile IDP, which is §3 item 2 | the IDP itself must validate signatures; Oak does not |
| Elasticsearch response (oak-search-elastic) | hit docs, shard metadata | only via a hostile ES, which is §3 item 2 | TLS / auth to ES is the operator's job |

### Size / shape / rate

- JCR allows arbitrary node hierarchies; Oak does not impose a maximum
  depth, child count, or property size by default. Pathological trees
  (millions of children in one node) degrade gracefully on the
  DocumentNodeStore but may exhibit poor cache behaviour
  *(inferred — §14 Q9)*. **No DoS protection is provided by
  default.**
- Query parser has no documented input-size cap; a 1 MB SQL2 query
  will be parsed *(inferred — §14 Q15)*.
- Observation events are delivered post-commit; back-pressure is the
  observer's responsibility *(documented: `security/permission.md`
  — "Events are only delivered once the modifications have been
  successfully persisted")*.

## §7 Adversary model

### Actors

| Actor | In scope? | Capabilities granted |
| --- | --- | --- |
| Authenticated end-user session (limited privileges) | **yes** | read/write the tree under their JCR privileges; submit SQL2/XPath queries; create/modify nodes within their licensed paths |
| Authenticated end-user session (broad but not admin privileges) | partial | only privilege-envelope escapes are in scope |
| Cross-session adversary (one session influencing another) | **yes** | cache poisoning, commit-hook side effects, permission-cache reuse across sessions *(documented: `security/permission.md` — "evaluated permissions and caches are not shared between different sessions")* |
| Authenticated user crafting pathological query / pathological tree | partial | DoS via slow query, oversized hierarchies — `VALID-HARDENING` at most *(inferred — §14 Q16)* |
| Hostile NodeStore / BlobStore peer (Mongo, S3, Tar files tampered with) | **out of scope** — §3 item 2 |
| Hostile external IDP (LDAP, SAML, OAuth) | **out of scope** — §3 item 2 |
| Host application code | **out of scope** — host is trusted by definition (§3 item 1) |
| Operator with shell access to the JVM host | **out of scope** — §3 item 3 / item 6 |
| Same-JVM attacker code (e.g., a malicious OSGi bundle co-installed in the same container) | **partial** *(inferred — §14 Q17)*: Oak runs in-process; a co-installed bundle that holds an unauthenticated reference to `Repository` can `loginAdministrative` — that is the host's container's problem, not Oak's |
| Side-channel observer (cache timing, JVM JIT timing) | **out of scope** *(inferred — §14 Q6)* |
| Quantum adversary | **out of scope** |

The model does **not** include the "authenticated-but-Byzantine peer" of
a consensus / replication system. The DocumentNodeStore on a Mongo
cluster is a Mongo-replication story; Oak trusts the Mongo answer
*(inferred — §14 Q2)*.

## §8 Security properties the project provides

### P1 — Authentication of JCR sessions through the configured `LoginContext` chain

- **Condition.** A `SecurityProvider` is configured (default
  `SecurityProviderImpl`); a `LoginContextProvider` is wired; the JAAS
  app-name resolves to a chain that includes at least one
  authenticating `LoginModule` *(documented:
  `security/authentication.md`)*.
- **Violation symptom.** A `Repository.login` call returns a `Session`
  for a `Credentials` object the configured chain should have
  rejected.
- **Severity.** Security-critical (`VALID`).
- *(documented)*

### P2 — Authorisation of reads through `PermissionProvider`

- **Condition.** A non-admin / non-system principal session; the
  configured `AuthorizationConfiguration` (default + optional CUG +
  optional principal-based) returns a `PermissionProvider` that
  evaluates per-tree `TreePermission.canRead()` *(documented:
  `security/permission/evaluation.md`,
  `security/permission/default.md`)*.
- **Violation symptom.** `Session.getNode(path)` /
  `Session.getProperty(path)` returns or surfaces an item the configured
  ACLs do not license; or a query result includes a node path the
  caller has no read permission for *(documented: query-result
  filtering, §4 reachability note for `oak-lucene`)*.
- **Severity.** Security-critical (`VALID`).
- *(documented)*

### P3 — Authorisation of writes through the `PermissionValidator` commit hook

- **Condition.** Non-admin / non-system principal session; default
  `PermissionValidator` wired into the commit-hook chain
  *(documented: `security/permission/evaluation.md`)*.
- **Violation symptom.** A `Session.save()` commits an `ADD_NODE`,
  `MODIFY_PROPERTY`, `REMOVE_NODE`, or AC-modify change the configured
  ACLs do not license.
- **Severity.** Security-critical (`VALID`).
- *(documented)*

### P4 — Hidden internal stores are not addressable through the public Oak / JCR API

- **Condition.** Default `PermissionProvider`; hidden items
  *(documented: `security/permission/default.md` — "Hidden items in
  Oak are excluded from this access grant ... system principals gain
  access except for hidden items that are not exposed on the Oak
  API")*.
- **Violation symptom.** A path under `/jcr:system/rep:permissionStore`
  is readable, writable, or enumerable through `Session`,
  `QueryManager`, or `Workspace` APIs.
- **Severity.** Security-critical (`VALID`).
- *(documented)*

### P5 — Permission-store immutability through JCR/Oak APIs

- **Condition.** Default `PermissionProvider`; `FailingValidator` in the
  commit-hook chain *(documented: `security/permission/default.md` —
  "A dedicated `FailingValidator` prevents modifications, and read
  access is restricted to system principals only")*.
- **Violation symptom.** A `Session.save()` mutates a node under
  `/jcr:system/rep:permissionStore`.
- **Severity.** Security-critical (`VALID`).
- *(documented)*

### P6 — Universally-readable system trees are intentional

- **Condition.** `/jcr:system/rep:namespaces`,
  `/jcr:system/jcr:nodeTypes`, `/jcr:system/rep:privileges` are
  readable to all subjects *(documented:
  `security/permission/default.md`)* — this is **stated as a property**
  because "many JCR API calls rely on the accessibility of the
  namespace, nodetype and privilege information".
- **Violation symptom.** A configured authorisation that denies read on
  these paths to authenticated principals breaks JCR API contract.
- **Severity.** Correctness (`VALID` only if a downstream JCR API call
  fails because of a deny that should never have been applied to
  these paths).
- *(documented)*

### P7 — Password hashing for stored Oak users

- **Condition.** `UserConfiguration` default; `PasswordUtil` configured
  with PBKDF2 *(documented: `security/user.md`)*.
- **Violation symptom.** A password is stored in plaintext, with a
  weak hash, or without a salt under the default configuration.
- **Severity.** Security-critical (`VALID`). On legacy users with a
  pre-PBKDF2 stored hash, the severity is **conditional on the §14
  Q13 ruling**.
- *(documented + inferred)*

### P8 — `TokenLoginModule` token integrity

- **Condition.** `TokenConfiguration` enabled (default); token TTL
  configured.
- **Violation symptom.** A token validates that was never issued, or a
  revoked token continues to validate after `TokenProvider.removeToken`.
- **Severity.** Security-critical (`VALID`).
- *(inferred — §14 Q18)*

### P9 — Per-session permission caches are not shared

- **Condition.** Default `PermissionProvider`; one provider instance per
  `Session` *(documented: `security/permission.md` — "Each JCR Session
  receives its own provider instance tied to the current repository
  revision, ensuring evaluated permissions and caches are not shared
  between different sessions even if they represent the same
  subject")*.
- **Violation symptom.** A permission decision made on Session A
  influences a permission decision made on Session B for the same
  principal.
- **Severity.** Security-critical (`VALID`).
- *(documented)*

### P10 — MVCC isolation: a session sees the revision captured at login

- **Condition.** Default NodeStore (Document, Segment, Composite); Oak
  MVCC layer.
- **Violation symptom.** A session reads a node version that was
  committed by another session strictly after the reading session's
  login, without an intervening refresh.
- **Severity.** Correctness-only by default. Security-critical iff the
  later-committed version contains an ACL that the reader should now
  be subject to and the reader bypasses it — but this is the
  *expected* MVCC behaviour and the ACL change is enforced on the
  reader's next refresh / new session.
- *(inferred — §14 Q19)*

### P11 — Memory safety of safe-Java core

- **Condition.** JVM-supported semantics; no JNI / `Unsafe` use in the
  core security paths.
- **Violation symptom.** OOB read/write, use-after-free, JVM crash
  reachable from a §6 input.
- **Severity.** This is normally not a Java concern; it lands as
  security-critical only when reachable through a JNI bridge (e.g. a
  vendored S3 SDK, oak-search-elastic transport bindings) where the
  underlying native code is in-scope only insofar as Oak's calling
  pattern triggers it *(inferred — §14 Q20)*.

## §9 Security properties the project does NOT provide

Stated plainly so triagers can route inbound reports to a matching
disclaimer.

### 9.1 No defence against the host application

The host that calls `loginAdministrative` or `loginService` *is the
system*. A host that exposes administrative sessions to network
peers, or that wires `Repository` into a network handler without an
authentication layer, has produced a bug at the host layer, not at
the Oak layer. *(documented: `security/permission/default.md`)*

### 9.2 No defence against a malicious NodeStore / BlobStore / IDP

Oak trusts whatever Mongo, RDB, Tar files, S3, Azure, LDAP, SAML, and
Elasticsearch return. *(inferred — §14 Q2)*

### 9.3 No isolation between in-JVM callers

Oak is a JCR library in the host JVM. A co-installed bundle (OSGi),
servlet, or thread can obtain a `Repository` reference and call
`loginAdministrative` if the host allows it. *(inferred —
§14 Q17)*

### 9.4 No DoS protection against authenticated callers

Oak provides no enforced cap on query complexity, tree depth, child
count, transient state per session, or commit size. A user with
`SELECT` and `MODIFY` privileges can submit a query or a save that
burns the host's CPU and memory budget. *(inferred — §14 Q15 /
Q16)*

### 9.5 No defence against query-result side channels

Query result filtering by `PermissionProvider` hides the *content* of
unauthorised nodes, but query timing, error messages, and partial
result counts may leak existence of unauthorised paths. *(inferred —
§14 Q21)*

### 9.6 No defender stance against pre-authentication misuse

`PreAuthenticatedLogin` is explicitly an "Oak does not verify"
mechanism. If the host invokes it on attacker-controlled bytes, Oak
will issue a session under the asserted identity. *(documented:
`security/authentication/preauthentication.md`)*

### 9.7 No defender stance against `loginAdministrative` / `loginService` exposure

These entry points are *trusted host-only* by design and bypass all
authorisation. If the host hands them out, Oak grants admin. *(documented:
`security/permission/default.md`)*

### 9.8 No "system principal" isolation

A `SystemPrincipal`, `AdminPrincipal`, or principal matching an
operator-configured admin name *bypasses permission evaluation
entirely*. There is no second gate. *(documented:
`security/permission/default.md`)*

### 9.9 No data-at-rest encryption at the Oak layer

The NodeStore writes whatever bytes the backend takes. SegmentNodeStore
writes plaintext tar files; DocumentNodeStore writes plaintext BSON /
SQL rows. At-rest encryption is delegated to the backend (Mongo at-rest
encryption, S3 SSE, FS-level encryption, etc.). *(inferred —
§14 Q22)*

### 9.10 CUG is read-only

`oak-authorization-cug` "solely evaluates and enforces read access to
regular nodes and properties" *(documented:
`security/authorization/cug.md`)*. A report that CUG fails to gate
*writes* is misreading the design.

### 9.11 No constant-time comparison of authentication secrets

Beyond what the underlying JCA / `PasswordUtil` / `TokenLoginModule`
helpers provide, Oak's `equals`-shaped comparisons are not documented
constant-time. *(inferred — §14 Q23)*

### 9.12 No defence against move-cascade permission edges

"Moving an ancestor of a node that has been moved will only detect
the second move and will enforce regular permissions checks on the
child." *(documented: `security/permission.md`)* This is a stated
limitation; a report against the cascade case is documented behaviour.

### 9.13 No defender stance against an authenticated user with `MODIFY_ACCESS_CONTROL`

A user with `jcr:modifyAccessControl` on a subtree can rewrite ACLs to
grant themselves more. This is *the* JCR access-control mutator
privilege. *(inferred — §14 Q24)*

### False-friend properties (call out separately)

- **`loginAdministrative` is not "log in as administrator after
  verifying credentials" — it is "no credential check at all, trust
  the caller".** The host must guard this entry point with its own
  authn layer.
- **The `admin` user existing by default is *not* a deployed-by-default
  weakness — it is documented; the operator is expected to rotate or
  remove the credentials.** Whether failing to do so is a `VALID` Oak
  report depends on the §14 Q14 ruling.
- **`ExternalLoginModule` does not validate IDP signatures.** It
  consults whatever the configured `ExternalIdentityProvider` says.
- **Pre-authentication "without repository involvement" is a
  zero-trust-on-Oak's-side mechanism.** Naming it "pre-authentication"
  may suggest Oak does a check; it does not.
- **`SystemPrincipal` access excluding "hidden items" is *not* a
  permission gate — hidden items are simply not surfaced on the Oak
  API.** A code path that reaches them through internal SPI is not
  constrained.
- **`oak-authorization-cug` is *not* a complete authorisation model —
  it only handles read-access restriction.** It must be composed with
  the default authz, not used alone.
- **A `TokenCredentials` token issued by Oak is a bearer credential.**
  Possession is sufficient until expiry or revocation; there is no
  audience or device binding.
- **`PrincipalProvider` is consulted for membership but is *not* an
  authorisation source.** A `PrincipalProvider` that adds the `admin`
  group to all principals is the host's bug, not Oak's.

### Well-known attack classes the project does not defend against

- **Confused-deputy via `loginAdministrative` /
  `PreAuthenticatedLogin`** — the host is the deputy.
- **Authenticated-DoS via expensive queries / large transient state**
  — admission control is the host's job (§9.4).
- **Pathological hierarchy / millions-of-children-on-one-node** —
  degrades gracefully, but no hard cap.
- **Time-of-check-to-time-of-use** between ACL fetch and tree write
  is bounded by the MVCC revision at session login (§8 P10), but
  cross-session "I see a write the other session committed *after* my
  ACL grant was revoked" is the expected MVCC behaviour.
- **JVM serialisation pitfalls** in code paths that load OSGi-supplied
  classes — that is host / container responsibility.

## §10 Downstream responsibilities

The host application (and, where relevant, the operator) MUST:

1. **Treat `loginAdministrative` and `loginService` as host-only entry
   points.** Never expose them to network peers, user-supplied input,
   or co-installed code that might invoke them indirectly *(documented:
   `security/permission/default.md`)*.
2. **Configure a `SecurityProvider`.** Without one, all sessions
   succeed without authentication and all reads/writes succeed
   without authorisation.
3. **Compose the AuthorizationConfiguration meaningfully.** If CUG or
   principal-based authz is required, register it via
   `CompositeAuthorizationConfiguration` *(documented:
   `security/authorization.md`)*. Be aware that CUG only enforces
   read restriction; pair it with the default authz for writes.
4. **Rotate or remove the default `admin` user** before production
   *(documented: `security/permission/default.md`)*.
5. **Pre-authentication is host-asserted.** If `PreAuthenticatedLogin`
   is in the chain, the host is fully responsible for ensuring the
   pre-auth signal comes from a trusted upstream verifier
   *(documented: `security/authentication/preauthentication.md`)*.
6. **Configure `TokenConfiguration` TTL** to match the data lifetime
   you are willing to expose to a stolen token.
7. **Use TLS to the NodeStore (Mongo, RDB), BlobStore (S3, Azure),
   external IDP (LDAP, SAML), and search backend (Elasticsearch).**
   Oak does not enforce transport encryption on these channels; the
   underlying driver / library does.
8. **Restrict OS filesystem permissions on `repository.home`,
   `oak-run` keystores, tar segments, and any `.crt`/`.key` material**
   to the Oak process user.
9. **Force password rehash on legacy accounts** so the
   `PasswordUtil` PBKDF2 path is applied (subject to §14 Q13).
10. **Apply admission controls at the host layer.** Oak imposes no
    cap on query complexity or save size; rate-limit / quota-limit
    upstream of `Session.save` and `QueryManager.createQuery`.
11. **Be aware that any principal added to the configured admin-name
    list bypasses authorisation.** Audit this list per release.
12. **Treat `oak-run` as offline tooling that holds full repository
    authority.** Anyone who runs `oak-run console` is `admin`.
13. **Do not enable `oak-it`, `oak-bench-*`, `oak-exercise`, or
    archived `oak-mk-*` in production.** They are unsupported.
14. **For the `MODIFY_ACCESS_CONTROL` privilege, audit which
    principals hold it on which subtrees.** A holder can grant
    themselves any privilege under that subtree.
15. **For external IDPs, validate the IDP itself** (LDAP TLS,
    LDAP-bind cert pinning, SAML signature verification at the IDP
    side, OAuth client secret hygiene). Oak honours whatever the IDP
    returns *(documented:
    `security/authentication/externalloginmodule.md`)*.

## §11 Known misuse patterns

- **Exposing the Oak `Repository` to a network handler without an
  authentication layer in front.** "I wrap Oak in a servlet, pass
  request bodies to `QueryManager.createQuery`, and return the
  results" — the *session* the servlet uses owns the authority. If
  it's an admin session, every request is admin.
- **Using `loginAdministrative` to "just bypass auth for a script".**
  Now the script's process boundary is the repository's boundary.
- **Registering `PreAuthenticatedLogin` without thinking about who
  controls the pre-auth signal.** A request header is sufficient if
  the operator forwards it.
- **Failing to compose CUG with the default authz, expecting CUG to
  also gate writes.** CUG is *read-only* by design.
- **Failing to rotate the default `admin` user.**
- **Adding a service identity to the administrative-principal name
  list and forgetting it.** That identity now bypasses authorisation
  globally.
- **Granting `MODIFY_ACCESS_CONTROL` over `/content` to a user-facing
  service principal.** That principal can rewrite ACLs to give
  itself more.
- **Configuring an LDAP IDP without TLS.** Credentials and group
  membership are now on the wire in cleartext.
- **Treating `TokenCredentials` as a session cookie scoped to a
  browser.** It is a bearer credential — anyone who has it can use
  it until expiry or revocation.
- **Co-installing untrusted OSGi bundles in the same OSGi container
  as Oak.** They share the JVM with Oak; they can `loginAdministrative`.
- **Using `loginService` with a `SubjectProvider` that derives its
  Subject from a query string.** Equivalent to a bypass.

## §11a Known non-findings (recurring false positives)

Highest-leverage input for automated agentic security scans. Each
entry: tool symptom, why it is safe under the model, the §x that
licenses the call.

- **"`loginAdministrative` performs no credential check."** This is
  documented and intentional — the host *is* the system *(documented:
  `security/permission/default.md`)*. → `BY-DESIGN: property-disclaimed`
  per §9.1.
- **"`PreAuthenticatedLogin` accepts identity without verification."**
  Documented — *(documented:
  `security/authentication/preauthentication.md`)*. → `BY-DESIGN:
  property-disclaimed` per §9.6.
- **"`admin` user exists by default with full repository access."**
  Documented; operator responsibility to rotate per §10. →
  `OUT-OF-MODEL: non-default-build` (subject to §14 Q14).
- **"`SystemPrincipal` bypasses permission evaluation."** Documented
  — *(documented: `security/permission/default.md`)*. → `BY-DESIGN:
  property-disclaimed` per §9.8.
- **"`oak-authorization-cug` does not enforce writes."** Documented
  — CUG is read-only *(documented: `security/authorization/cug.md`)*.
  → `BY-DESIGN: property-disclaimed` per §9.10.
- **"DoS via expensive SQL2 query / millions of children on a single
  node."** No DoS protection by design *(inferred — §14 Q15 /
  Q16)*. → `BY-DESIGN: property-disclaimed` per §9.4.
- **"LDAP credentials sent in cleartext when oak-auth-ldap is
  configured without TLS."** TLS is the operator's job *(inferred —
  §14 Q10)*. → `OUT-OF-MODEL: non-default-build`.
- **"`ExternalLoginModule` does not validate the IDP's signature."**
  IDP impl owns that check *(documented:
  `security/authentication/externalloginmodule.md`)*. →
  `OUT-OF-MODEL: trusted-input`.
- **"`oak-run console` dumps the entire repository without
  authentication."** `oak-run` is offline operator tooling — §3
  item 6. → `OUT-OF-MODEL: adversary-not-in-scope`.
- **"Hardcoded test password / keystore in `oak-it/`,
  `oak-exercise/`."** Unsupported components. → `OUT-OF-MODEL:
  unsupported-component`.
- **"Archived `oak-mk-*` modules contain insecure code."** Archived.
  → `OUT-OF-MODEL: unsupported-component`.
- **"Move-cascade ACL evaluation gap."** Documented limitation
  *(documented: `security/permission.md`)*. → `BY-DESIGN:
  property-disclaimed` per §9.12.
- **"Query result count leaks existence of unauthorised paths via
  timing / error message."** Side-channel — §9.5. → `BY-DESIGN:
  property-disclaimed`.
- **"`TokenCredentials` is a bearer credential — anyone who has it
  can use it."** Documented behaviour; rotation / revocation is the
  operator's. → `BY-DESIGN: property-disclaimed` per §9 false-friend.
- **"OSGi co-installed bundle obtains `loginAdministrative` without
  authn."** Host container is the security boundary, not Oak
  *(inferred — §14 Q17)*. → `OUT-OF-MODEL:
  adversary-not-in-scope`.

## §12 Conditions that would change this model

- A new `LoginModule` that ships in the default Oak chain (the chain
  composition determines P1).
- A new `AuthorizationConfiguration` that ships enabled by default
  (the composition determines P2 / P3).
- A change in the default password-hash scheme in `PasswordUtil`
  (affects P7 + §14 Q13).
- A network-facing component shipped in Oak proper (today Oak ships
  no listener — that would invalidate §2 deployment shape).
- A change in the default of any §5a knob — especially the
  administrative-principal name list.
- A new authentication mechanism (mTLS-as-auth, OIDC at the Oak
  layer, U2F).
- A new authorisation provider class joining the default composite.
- Promotion of `oak-it`, `oak-exercise`, `oak-mk-*`, or any
  `oak-bench-*` back into the supported product surface.
- A vulnerability report that cannot be cleanly routed to one of the
  §13 dispositions — that is evidence the model is incomplete.

## §13 Triage dispositions

A report against Oak receives exactly one of the following:

| Disposition | Meaning | Licensed by |
| --- | --- | --- |
| `VALID` | Violates a §8 property via an in-scope §7 adversary using an in-scope §6 input. | §8, §6, §7 |
| `VALID-HARDENING` | No §8 property violated, but a §11 misuse pattern can be made harder by code change. Typically no CVE. | §11 |
| `OUT-OF-MODEL: trusted-input` | Requires attacker control of a §6 parameter the model marks trusted (host config, IDP response, NodeStore byte stream). | §6 |
| `OUT-OF-MODEL: adversary-not-in-scope` | Requires a §7 actor the model excludes (host application, operator, hostile backend/IDP, side-channel observer, JVM co-resident). | §7 |
| `OUT-OF-MODEL: unsupported-component` | Lands in `oak-it/`, `oak-bench-*/`, `oak-exercise/`, `oak-examples/`, archived `oak-mk-*`, etc. | §3 item 7 |
| `OUT-OF-MODEL: non-default-build` | Manifests only under a discouraged or non-default §5a value (custom SecurityProvider; `admin` not rotated). | §5a |
| `BY-DESIGN: property-disclaimed` | Concerns a §9 property the project explicitly does not provide (`loginAdministrative`, `PreAuthenticatedLogin`, CUG-write, DoS, side channels, etc.). | §9 |
| `KNOWN-NON-FINDING` | Matches a §11a recurring false positive. | §11a |
| `MODEL-GAP` | Cannot be cleanly routed to any of the above — triggers §12 model revision. | §12 |

## §14 Open questions for the maintainers

Every *(inferred)* tag in the body maps to one of these. Proposed
answers are inline; confirm, correct, or strike.

### Wave 1 — scope and deployment shape

**Q1.** The model assumes Oak is an *in-process JCR library* with
**no built-in network listener**. Any network exposure (HTTP, WebDAV,
custom protocol) is host-driven. Confirm? *(maps to §2)*

**Q2.** Are NodeStore (Mongo, RDB, Tar), BlobStore (S3, Azure, FS),
and external IDPs (LDAP, SAML) modelled as trusted backends — i.e.
Oak does not defend against a malicious peer in those positions
(proposed: yes)? *(maps to §2, §3 item 2, §9.2, §11a)*

**Q3.** Filesystem-level protection of `repository.home`, tar
segments, FileBlobStore directories, and keystores — confirmed to be
the operator's responsibility, not Oak's? *(maps to §3 item 3,
§10 item 8)*

**Q4.** `oak-run` invoked as a CLI by an operator: confirmed
out-of-model (it requires OS-level credentials and direct filesystem
access)? *(maps to §3 item 6)*

**Q5.** `jackrabbit-filevault` package import: confirmed
out-of-model (filevault has its own threat model; Oak honours the
JCR session privileges that filevault uses but does not own its
trust posture)? *(maps to §3 item 9)*

**Q6.** Side-channel adversaries (cache timing, JIT timing,
co-tenant on the JVM): confirmed out-of-model? *(maps to §7, §9.5)*

### Wave 2 — index, query, and search

**Q7.** Does the Oak `QueryEngine` filter search results by the
caller's `PermissionProvider` *before* result delivery, such that a
query result cannot surface a path the caller has no read permission
for? Proposed: yes — but a query *timing* / *count* side channel may
leak existence (§9.5). Confirm? *(maps to §4 oak-lucene, §8 P2,
§9.5)*

### Wave 3 — environment

**Q8.** `Session` is documented to be per-thread (not thread-safe);
`Repository`/`ContentRepository` are thread-safe. Confirm? *(maps to
§5)*

**Q9.** Resource bounds in caches and tree shapes: confirmed that Oak
imposes no hard cap on cache size, tree depth, child count, or
property size by default — pathological shapes degrade gracefully
but are not security failures? *(maps to §5, §6, §9.4)*

**Q10.** Outbound connections (Mongo / RDB / S3 / Azure / LDAP / ES)
are all operator-configured trusted endpoints; TLS is the backing
driver's responsibility, not Oak's? *(maps to §5)*

**Q11.** "What Oak does NOT do to its host" inventory: no listening
sockets, no signal handlers, no child processes, no unbounded
syspropery reads. Confirm any inaccuracies. *(maps to §5)*

### Wave 4 — §5a defaults and admin posture

**Q12.** §5a `SecurityProvider` composition. Each knob in the
table is currently tagged either "default" or "optional" — for each,
confirm whether the documented default is the *supported production
posture* (so a violation in default config is `VALID`) or a
*dev/test default* (so the violation is `OUT-OF-MODEL: non-default-build`).
*(maps to §5a, §10, §13)*

**Q13.** Pre-PBKDF2 hashed passwords on legacy users — what is the
project's stance? Proposed: legacy hashes remain readable for
backwards compat but operators are expected to force a rehash on
login. Is "user X still has a SHA-1 stored hash because they have
not logged in since 2017" a `VALID` Oak report or an operator
posture issue? *(maps to §5a, §8 P7, §10 item 9)*

**Q14.** The default `admin` user has full repository access.
Proposed disposition for "deployment X has not rotated the default
`admin`": `OUT-OF-MODEL: non-default-build` (operator
responsibility). Confirm or correct. *(maps to §5a, §10 item 4,
§11 first bullet, §11a)*

### Wave 5 — query, DoS, MVCC

**Q15.** Query parser input-size cap: is there one (proposed: no)?
*(maps to §6, §9.4)*

**Q16.** "Authenticated user submits a pathological SQL2 query that
burns CPU/memory" — confirmed `BY-DESIGN: property-disclaimed`
(admission control is the host's job)? Same question for
pathological tree shapes during `save()`. *(maps to §7, §9.4,
§11a)*

**Q17.** Same-JVM (e.g. OSGi co-installed bundle) attackers:
confirmed out-of-model? Oak runs in-process; a co-resident untrusted
bundle that obtains a `Repository` reference can `loginAdministrative`.
The host container is the security boundary. *(maps to §7, §9.3)*

**Q18.** `TokenLoginModule` token integrity (§8 P8): is the
property "an issued token validates iff it has not been revoked or
expired" formally upheld? Where is the test surface? *(maps to §8 P8)*

**Q19.** MVCC visibility (§8 P10): "a session sees the revision
captured at login" — is `Session.refresh(true)` the standard advance
mechanism, and is it the *only* one (background changes between
login and explicit refresh are invisible)? *(maps to §8 P10, §9.5)*

**Q20.** Memory safety of safe-Java (§8 P11): are there JNI bridges
in the Oak core (oak-segment-tar's native paths, oak-search-elastic
transport client) where a memory-safety bug in the underlying native
code is *reachable* from in-Oak code? Should those be called out
under their own component-family trust level? *(maps to §4, §8 P11)*

### Wave 6 — false friends and edge cases

**Q21.** Query / search side channels: confirmed that "a query
timing variation that distinguishes an authorised-but-empty result
from an unauthorised hidden result" is `BY-DESIGN:
property-disclaimed` and not a §8 violation? *(maps to §9.5)*

**Q22.** Data-at-rest encryption: confirmed delegated to backend
(Mongo encryption-at-rest, S3 SSE, FS-level encryption)? *(maps to
§9.9)*

**Q23.** Constant-time comparison of authentication secrets: is there
anything in Oak that should be constant-time and currently is not,
or is all such logic delegated to `PasswordUtil` /
`TokenLoginModule` / the JCA? *(maps to §9.11)*

**Q24.** `MODIFY_ACCESS_CONTROL` is the privilege that lets a holder
rewrite ACLs to grant themselves more. Confirmed that this is
intended behaviour and that "user X has MAC and used it to escalate"
is `OUT-OF-MODEL: equivalent-harm`-shaped — i.e. the privilege does
what it says? *(maps to §9.13)*

### Wave 7 — meta

**Q25.** Should this document live as `oak-doc/src/site/markdown/security/threat-model.md`
(proposed, alongside `reports.md`)? *(meta)*

**Q26.** Is there an existing Oak threat-model document (Confluence,
internal, or in the in-repo `AGENTS.md` per the SKILL §3.1a
co-existence rule) that this should reconcile with rather than
supersede? *(meta — §3.1a of the rubric)*

**Q27.** §11a known-non-findings is well-populated from docs but
not from JIRA. Could the PMC contribute 3–5 patterns the Oak triage
queue sees recur in inbound reports (e.g. "session.refresh did not
pick up a write the reporter expected", "CUG denied a write — by
design", "loginAdministrative used from a host integration is not a
bug") to harden §11a? Public Oak security artefacts (the
security/reports.md page lists one CVE) carry essentially no
project-direct triage history to mine. *(meta — §11a)*

**Q28.** Should this Oak model cross-reference the (still-to-be-
drafted) `jackrabbit` and `jackrabbit-filevault` models for items §3
items 8–9, or stand alone? *(meta)*

---

## Appendix: Existing security artefact → §x back-map

Oak does not currently ship an in-repo `SECURITY.md`. The de-facto
security-policy artefacts are the in-repo Markdown under
`oak-doc/src/site/markdown/security/` (published as the
`https://jackrabbit.apache.org/oak/docs/security/` documentation
tree) plus `oak-doc/src/site/markdown/security/reports.md` (the CVE
disclosure page).

| Source | Claim | Lands in |
| --- | --- | --- |
| `security/introduction.md` | "only recommended for experts ... understand the security risk associated with custom replacements" | §3 item 5 |
| `security/overview.md` | enumeration of the 6 security topics + the `SecurityProvider` model | §2 component family, §4 trust boundaries |
| `security/authentication.md` | JAAS chain, `LoginContextProvider`, `LoginModule` semantics | §4 B1, §8 P1 |
| `security/authentication/externalloginmodule.md` | "implicitly trusts that the configured IDP accurately authenticates identities" | §3 item 2, §4 B5, §11a |
| `security/authentication/preauthentication.md` | "Oak delegates all authentication responsibility to the caller" | §3 item 4, §9.6, §11a |
| `security/authorization.md` | `AccessControlManager` / `PermissionProvider` separation; `CompositeAuthorizationConfiguration` | §4 B3/B4, §5a, §8 P2/P3 |
| `security/permission.md` | per-session `PermissionProvider`; "permissions and caches are not shared between sessions"; move-cascade limitation; observation event ordering | §8 P9, §9.12 |
| `security/permission/default.md` | universally-readable system trees; `SystemPrincipal`/`AdminPrincipal` bypass; permission-store immutability; hidden items not exposed | §8 P4/P5/P6, §9.8 |
| `security/permission/evaluation.md` | `SecureNodeBuilder` wrapping; `PermissionValidator` commit-hook; admin bypass | §8 P2/P3 |
| `security/authorization/cug.md` | CUG is read-only; not for default mounts intersecting supported paths; specific privileges only | §3, §8 P2, §9.10 |
| `security/user.md` | `PasswordUtil` PBKDF2; `Authorizable`/`User`/`Group`; `Impersonation` | §8 P7, §5a |
| `security/reports.md` | CVE-2020-1940 + "binary patches are not produced" | §1 reporting cross-ref |
| `AGENTS.md` | module list; security-module 100% coverage mandate; `FT_OAK-<issue>` toggles; no regex parsing | §2 component family, §5a |
| `.asf.yaml` | `trunk` protected; `oak-dev@jackrabbit.apache.org` / `oak-commits@jackrabbit.apache.org` | §1 |
| `README.md` | MicroKernel modules archived | §2 / §3 item 7 |
