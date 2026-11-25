# OAK-12495: Monitor admin user operations — implementation plan

Module: `oak-core` (packages `o.a.j.oak.security.user` and `...user.monitor`).
`...user.monitor` is not exported (no package-info, not in Export-Package) → no baseline impact.
Coverage gates: `user.monitor` 100% branch, `user` 99% branch.

## Scope
Report the following events of existing users to `UserMonitor` (generic API, each call carries `isAdmin`, see
section 2):
1. login (password based, `SimpleCredentials`)
2. being impersonated (user is impersonation target)
3. password added (none before) / changed (removal of `rep:password` is already rejected by `UserValidator`, OAK0025)
4. impersonators granted / revoked (`rep:impersonators`)

The default `UserMonitorImpl` records metrics for the admin user only (as identified by configured `adminId`,
`UserUtil.isAdmin` / `UserValidator#isAdminUser`), i.e. for calls with `isAdmin=true`.

## Out of scope
- Admin acting as impersonator of other users (admin can do anything anyway).
- Token-based admin login (`TokenAuthentication`), guest / pre-authenticated logins, system subjects with `AdminPrincipal` / `SystemPrincipal`, Oak system sessions.
- Custom `UserAuthenticationFactory` services (take precedence via `WhiteboardUserAuthenticationFactory`; logins not
  handled by the default `UserAuthentication` are not recorded).
- Other administrative aspects (e.g. administrative-principals configuration).
- Changes bypassing commit hooks (e.g. offline tooling writing directly to the NodeStore).
- Capturing the user agent (or other request details) of the caller. Oak has no request context, meters have
  no labels (and a client-controlled value would be unbounded and spoofable). Correlate with the HTTP access
  log instead; an audit log in Oak relying on the logging MDC set by the web layer could be a separate issue.

## 1. Metric type
Meters (`MeterStats`, `StatsOptions.DEFAULT`), consistent with existing `UserMonitorImpl`.
Prometheus export: `oak_security_user_admin_*_total` → alerts via `increase(...)`/`rate(...)`.

## 2. API: `UserMonitor` (new no-op default methods)
```java
default void login(boolean success, boolean isAdmin) {}
default void impersonated(boolean success, boolean isAdmin) {}
default void passwordChanged(boolean added, boolean isAdmin) {}
default void impersonatorsChanged(long granted, long revoked, boolean isAdmin) {}
```
All methods are called for every (existing) user (generic, prepares for monitoring all users later);
`UserMonitorImpl` only records them if `isAdmin` is `true`.

## 3. Impl: `UserMonitorImpl` — meters
| Event | Meter |
|---|---|
| admin login ok / failed | `security.user.admin.login.succeeded` / `.failed` |
| admin impersonated ok / failed | `security.user.admin.impersonation.succeeded` / `.failed` |
| admin pw added / changed | `security.user.admin.password.added` / `.changed` |
| impersonators of admin granted / revoked | `security.user.admin.impersonators.granted` / `.revoked` (both always marked with the respective count, possibly 0) |

## 4. Content changes — `UserValidator` (Oak layer, commit time)
Rationale: covers every write path (user mgt API, XML import, expired-pw change at login, plain Oak
`Root`/`Tree` API where `rep:password`/`rep:impersonators` are not protected); counts changes observed during
commit validation (best-effort, see 4.3).

### 4.1 Wiring
- `UserConfigurationImpl.getValidators(...)` → `new UserValidatorProvider(..., monitor)`;
  `UserValidatorProvider` passes it to `UserValidator` (via provider getter, like `getConfig()`).
- `getValidators` is invoked per commit (`MutableRoot.getCommitHook`) → current monitor read each commit;
  commits before `getMonitors()` use NOOP (acceptable). Make `UserConfigurationImpl.monitor` `volatile`.
  No setter needed.

### 4.2 Detection in `UserValidator` (parent tree = existing user node, `isAdmin = isAdminUser(parentAfter)`)
| Callback | Property | Record |
|---|---|---|
| `propertyAdded` | `rep:password` | `passwordChanged(true, isAdmin)` (e.g. admin created with `omitAdminPw` gets a pw) |
| `propertyChanged` | `rep:password` | `passwordChanged(false, isAdmin)` |
| `propertyAdded` | `rep:impersonators` | `impersonatorsChanged(after.size, 0, isAdmin)` |
| `propertyChanged` | `rep:impersonators` | diff before/after value sets → `(added, removed, isAdmin)` |
| `propertyDeleted` | `rep:impersonators` | `impersonatorsChanged(0, before.size, isAdmin)` |
- Groups and other non-user nodes → not reported.
- New user node (`childNodeAdded`, e.g. `UserInitializer`, import creating admin) → the child validator
  is created with `parentBefore == null` → not reported.
- Existing non-user node converted into a user (e.g. folder → `rep:User`) → `parentBefore` is not a user → not
  reported (treated like creation).
- Import with UUID behavior REMOVE/REPLACE_EXISTING recreating the admin node at the same path appears as
  a node change in the diff → pw change reported if the imported hash differs (not covered by a dedicated test).
- Only record after the existing checks in the callback pass (don't count changes rejected by these property
  checks; rejections later in the commit are covered in 4.3).

### 4.3 Caveats
- Commit hooks may be re-run on merge retries (Segment/Document) → rare double counting; acceptable.
- A later validator/hook may still reject the commit after recording → rare over-counting; acceptable.
  Covered by `AdminUserMonitorTest#testAdminPasswordChangeCommitFailsAfterRecording`.
- Hence the metrics support alerting and investigation; they do not prove persistence and are no audit trail.
  Login/impersonation metrics (section 5) record attempts and are not subject to these caveats.

### 4.4 Note: plain-text password added (pre-existing behavior)
- `UserValidator` rejects a plain-text `rep:password` only in `propertyChanged` (OAK0024), not in `propertyAdded`.
  Adding a plain-text `rep:password` via the Oak API to an existing user without one (e.g. an `omitAdminPw` admin)
  is therefore not rejected and, for the admin, is counted as `password.added`.
- A stored plain-text value never matches at login (`PasswordUtil.isSame` returns `false`), so this does not
  enable password login; the gap concerns integrity of the stored value (unhashed), not authentication.
- Decision: not fixed here. This is a pre-existing validation gap unrelated to monitoring; changing it alters
  commit behavior and should be tracked in a separate issue.

## 5. Login — `UserAuthentication.authenticate` (not a content change)
- Monitor access: the `UserAuthentication` constructor (signature unchanged) uses
  `((UserConfigurationImpl) config).getMonitor()` if `config instanceof UserConfigurationImpl`,
  else `UserMonitor.NOOP` (new package-private getter).
- Reported once in the `finally` block of `authenticate` (`monitorAuthenticate`), after `removeNewPwAttribute`, if
  `getValidUser(...)` returned a user (admin or not); `isAdmin = user.isAdmin()` is evaluated there. The disabled
  check is moved from `getValidUser` to `authenticate` (same exception and order), so that the user is known when
  it fails. `success` is the decision of
  `UserAuthentication`, not the outcome of the overall JAAS login:
  - `SimpleCredentials`: `login(success, user.isAdmin())`. Failure = disabled user (`AccountLockedException`), pw mismatch,
    or expired pw without successful change (`CredentialExpiredException`).
  - `ImpersonationCredentials`: `impersonated(success, user.isAdmin())`, including admin self-impersonation. Failure =
    disabled user or impersonation not allowed.
  - Guest / pre-auth: not recorded (also for disabled users).
  - Unknown id or group (`getValidUser` returns null / throws `AccountNotFoundException`): not recorded.
  - A `RepositoryException` (rethrown as `LoginException`) resets `success` to `false`, also after the credentials
    were verified.
  - The admin cannot be disabled, so disabled-user attempts never mark admin meters.

## 6. Feature toggle
Pure additive monitoring with NOOP defaults → no toggle.

## 7. Tests (JUnit4 + Mockito; real `UserMonitorImpl` with a mocked `StatisticsProvider` returning per-name meters)
- `UserMonitorImplTest`: each new method marks the right meters (100% branch); non-admin calls mark nothing.
- `AdminUserMonitorTest` (validator): admin pw added (validator level)/changed, impersonators added/changed
  (grant+revoke diff)/removed, non-admin user (pw, impersonators) → no admin meter marked, non-admin pw added/changed and
  impersonators added/changed/removed reported with `isAdmin=false`, group / folder converted to user → not reported, other admin property → no
  admin meter marked, uncommitted change → no interaction, user creation / new admin node → no interaction, plain-text
  pw change rejected → no interaction, commit failing after recording → meter marked but pw unchanged;
  via user mgt API and plain Tree API.
  (Password removal rejected → OAK0025 is covered by the existing `UserValidatorTest`.)
- `AdminUserMonitorOmitPasswordTest`: admin set up with `omitAdminPw` → pw added, then changed.
- `UserImporterAdminMonitorTest`: rep:password / rep:impersonators import on existing admin → recorded.
- `AdminUserMonitorTest` (authentication): admin login ok/fail (+ expired pw), admin impersonated ok/fail,
  self-impersonation recorded, non-admin not recorded, non-`UserConfigurationImpl` → NOOP, monitor receives
  `isAdmin` for admin and non-admin login/impersonation (succeeded and failed), disabled user login/impersonation
  → reported as failed, guest / pre-auth (incl. disabled user), unknown id and group → no interaction, `RepositoryException` after credential verification → reported as failed.
- Run: `mvn test -pl oak-core -Dtest='AdminUserMonitor*Test,UserMonitorImplTest,UserValidator*Test,UserAuthentication*Test,UserImporter*Test'`,
  then `mvn verify -pl oak-core -Pcoverage -Dskip.coverage=false`.

## 8. Docs
User management docs in `oak-doc/src/site/markdown/security/user/`: add "Monitoring" section listing
all `security.user.*` meters (existing membership metrics are undocumented as well).
While documenting, `UserMonitorImpl.doneGetMembers` was found to swap `get_members.timer` and
`get_declared_members.timer` (review feedback). Not fixed here, tracked in
[OAK-12550](https://issues.apache.org/jira/browse/OAK-12550).

## Open questions
None.
