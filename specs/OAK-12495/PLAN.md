# OAK-12495: Monitor admin user operations — implementation plan

Module: `oak-core` (packages `o.a.j.oak.security.user` and `...user.monitor`).
`...user.monitor` is not exported (no package-info, not in Export-Package) → no baseline impact.
Coverage gates: `user.monitor` 100% branch, `user` 99% branch.

## Scope
Admin user only (as identified by configured `adminId`, `UserUtil.isAdmin` / `UserValidator#isAdminUser`):
1. admin login (password based, `SimpleCredentials`)
2. admin being impersonated (admin is impersonation target)
3. admin password added (none before) / changed (removal of `rep:password` is already rejected by `UserValidator`, OAK0025)
4. impersonators of admin granted / revoked (`rep:impersonators` on admin)

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
default void adminLogin(boolean success) {}
default void adminImpersonated(boolean success) {}
default void adminPasswordChanged(boolean added) {}
default void adminImpersonatorsChanged(long granted, long revoked) {}
```

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

### 4.2 Detection in `UserValidator` (parent tree = user node, `isAdminUser(parentAfter/parentBefore)`)
| Callback | Property | Record |
|---|---|---|
| `propertyAdded` | `rep:password` | `adminPasswordChanged(true)` (e.g. admin created with `omitAdminPw` gets a pw) |
| `propertyChanged` | `rep:password` | `adminPasswordChanged(false)` |
| `propertyAdded` | `rep:impersonators` | `adminImpersonatorsChanged(after.size, 0)` |
| `propertyChanged` | `rep:impersonators` | diff before/after value sets → `(added, removed)` |
| `propertyDeleted` | `rep:impersonators` | `adminImpersonatorsChanged(0, before.size)` |
- New admin node (`childNodeAdded`, e.g. `UserInitializer`, import creating admin) → the child validator
  is created with `parentBefore == null` → not counted.
- Import with UUID behavior REMOVE/REPLACE_EXISTING recreating the admin node at the same path appears as
  a node change in the diff → pw change counted if the imported hash differs (not covered by a dedicated test).
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
- After `getValidUser(...)` returns a user with `user.isAdmin()`:
  - `SimpleCredentials`: `adminLogin(success && !expired)`, recorded before `checkSuccess` throws;
    `expired = success && isPasswordExpired(user) && !changePassword(user, creds)`. Failure = pw mismatch or
    expired pw without successful change (`CredentialExpiredException`).
  - `ImpersonationCredentials`: `adminImpersonated(success)`, including admin self-impersonation.
  - Guest / pre-auth: not recorded.

## 6. Feature toggle
Pure additive monitoring with NOOP defaults → no toggle.

## 7. Tests (JUnit4 + Mockito; real `UserMonitorImpl` with a mocked `StatisticsProvider` returning per-name meters)
- `UserMonitorImplTest`: each new method marks the right meters (100% branch).
- `AdminUserMonitorTest` (validator): admin pw added (validator level)/changed, impersonators added/changed
  (grant+revoke diff)/removed, non-admin user (pw, impersonators) → no interaction, other admin property → no
  interaction, uncommitted change → no interaction, user creation / new admin node → no interaction, plain-text
  pw change rejected → no interaction, commit failing after recording → meter marked but pw unchanged;
  via user mgt API and plain Tree API.
  (Password removal rejected → OAK0025 is covered by the existing `UserValidatorTest`.)
- `AdminUserMonitorOmitPasswordTest`: admin set up with `omitAdminPw` → pw added, then changed.
- `UserImporterAdminMonitorTest`: rep:password / rep:impersonators import on existing admin → recorded.
- `AdminUserMonitorTest` (authentication): admin login ok/fail (+ expired pw), admin impersonated ok/fail,
  self-impersonation recorded, non-admin not recorded, non-`UserConfigurationImpl` → NOOP.
- Run: `mvn test -pl oak-core -Dtest='AdminUserMonitor*Test,UserMonitorImplTest,UserValidator*Test,UserAuthentication*Test,UserImporter*Test'`,
  then `mvn verify -pl oak-core -Pcoverage -Dskip.coverage=false`.

## 8. Docs
User management docs in `oak-doc/src/site/markdown/security/user/`: add "Monitoring" section listing
all `security.user.*` meters (existing membership metrics are undocumented as well).

## 9. Follow-up (outside Oak)
skyline-monitoring: alerts on `oak_security_user_admin_*_total`
(e.g. `increase(oak_security_user_admin_login_failed_total[15m]) > 0`, admin impersonated,
admin pw added/changed, impersonators granted/revoked).

## Open questions
None.
