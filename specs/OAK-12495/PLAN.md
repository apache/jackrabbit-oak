# OAK-12495: Monitor admin user operations — implementation plan

Module: `oak-core` (packages `o.a.j.oak.security.user` and `...user.monitor`).
`...user.monitor` is not exported (no package-info, not in Export-Package) → no baseline impact.
Coverage gates: `user.monitor` 100% branch, `user` 99% branch.

## Scope
Admin user only (as identified by configured `adminId`, `UserUtil.isAdmin` / `UserValidator#isAdminUser`):
1. admin login (password based, `SimpleCredentials`)
2. admin being impersonated (admin is impersonation target)
3. admin password changed (removal of `rep:password` is already rejected by `UserValidator`, OAK0025)
4. impersonators of admin granted / revoked (`rep:impersonators` on admin)

## Out of scope (separate issues)
- Admin acting as impersonator of other users (admin can do anything anyway); admin self-impersonation.
- Token-based admin login (`TokenAuthentication`), guest / pre-authenticated logins, system login.
- Other administrative aspects (e.g. administrative-principals configuration).
- Changes bypassing commit hooks (e.g. offline tooling writing directly to the NodeStore).

## 1. Metric type
Meters (`MeterStats`, `StatsOptions.DEFAULT`), consistent with existing `UserMonitorImpl`.
Prometheus export: `oak_security_user_admin_*_total` → alerts via `increase(...)`/`rate(...)`.

## 2. API: `UserMonitor` (new no-op default methods)
```java
default void adminLogin(boolean success) {}
default void adminImpersonated(boolean success) {}
default void adminPasswordChanged() {}
default void adminImpersonatorsChanged(long granted, long revoked) {}
```

## 3. Impl: `UserMonitorImpl` — meters
| Event | Meter |
|---|---|
| admin login ok / failed | `security.user.admin.login.succeeded` / `.failed` |
| admin impersonated ok / failed | `security.user.admin.impersonation.succeeded` / `.failed` |
| admin pw set/changed | `security.user.admin.password.changed` |
| impersonators of admin granted / revoked | `security.user.admin.impersonators.granted` / `.revoked` (marked with counts; skip `mark(0)`) |

## 4. Content changes — `UserValidator` (Oak layer, commit time)
Rationale: covers every write path (user mgt API, XML import, expired-pw change at login, plain Oak
`Root`/`Tree` API where `rep:password`/`rep:impersonators` are not protected); only counts committed changes.

### 4.1 Wiring
- `UserConfigurationImpl.getValidators(...)` → `new UserValidatorProvider(..., monitor)`;
  `UserValidatorProvider` passes it to `UserValidator` (via provider getter, like `getConfig()`).
- `getValidators` is invoked per commit (`MutableRoot.getCommitHook`) → current monitor read each commit;
  commits before `getMonitors()` use NOOP (acceptable). Make `UserConfigurationImpl.monitor` `volatile`.
  No setter needed.

### 4.2 Detection in `UserValidator` (parent tree = user node, `isAdminUser(parentAfter/parentBefore)`)
| Callback | Property | Record |
|---|---|---|
| `propertyAdded` | `rep:password` | `adminPasswordChanged()` (e.g. admin created with `omitAdminPw` gets a pw) |
| `propertyChanged` | `rep:password` | `adminPasswordChanged()` |
| `propertyAdded` | `rep:impersonators` | `adminImpersonatorsChanged(after.size, 0)` |
| `propertyChanged` | `rep:impersonators` | diff before/after value sets → `(added, removed)` |
| `propertyDeleted` | `rep:impersonators` | `adminImpersonatorsChanged(0, before.size)` |
- New admin node (`childNodeAdded`, e.g. `UserInitializer`, import creating admin) → properties are
  validated as part of a new subtree, not via property callbacks on an existing node → not counted. Verify.
- Import with UUID behavior REMOVE/REPLACE_EXISTING recreating the admin node at the same path appears as
  a node change in the diff → pw change counted. Verify in test.
- Only record after the existing checks in the callback pass (don't count changes the validator rejects).

### 4.3 Caveats
- Commit hooks may be re-run on merge retries (Segment/Document) → rare double counting; acceptable.
- A later validator/hook may still reject the commit after recording → rare over-counting; acceptable.

## 5. Login — `UserAuthentication.authenticate` (not a content change)
- Monitor access: `UserAuthentication` gets a `UserMonitor` ctor param; `UserAuthenticationFactoryImpl`
  passes `((UserConfigurationImpl) cfg).getMonitor()` if `cfg instanceof UserConfigurationImpl`,
  else `UserMonitor.NOOP` (new package-private getter).
- After `getValidUser(...)` returns a user with `user.isAdmin()`:
  - `SimpleCredentials`: `adminLogin(success)`; failure = pw mismatch (record before `checkSuccess` throws).
    Expired pw without successful change (`CredentialExpiredException`) → failed.
  - `ImpersonationCredentials`: `adminImpersonated(success)`, skipped when impersonator is the admin itself.
  - Guest / pre-auth: not recorded.

## 6. Feature toggle
Pure additive monitoring with NOOP defaults → no toggle.

## 7. Tests (JUnit4 + Mockito, mock `UserMonitor` like `AbstractUserTest`)
- `UserMonitorImplTest`: each new method marks the right meters, zero counts not marked (100% branch).
- `UserValidator*Test` (new `UserValidatorAdminMonitorTest`): admin pw added/changed, removal rejected → no interaction,
  impersonators added/changed (grant+revoke diff)/removed, non-admin user → no interaction,
  admin creation → no interaction, rejected change → no interaction; via user mgt API and plain Tree API.
- `UserImporter*Test`: rep:password / rep:impersonators import on existing admin → recorded.
- `UserAuthentication*Test`: admin login ok/fail (+ expired pw), admin impersonated ok/fail,
  self-impersonation not recorded, non-admin not recorded, non-`UserConfigurationImpl` → NOOP.
- Run: `mvn test -pl oak-core -Dtest='UserMonitorImplTest,UserValidator*Test,UserAuthentication*Test,UserImporter*Test'`,
  then `mvn verify -pl oak-core -Pcoverage -Dskip.coverage=false`.

## 8. Docs
User management docs in `oak-doc/src/site/markdown/security/user/`: add "Monitoring" section listing
all `security.user.*` meters (existing membership metrics are undocumented as well).

## 9. Follow-up (outside Oak)
skyline-monitoring: alerts on `oak_security_user_admin_*_total`
(e.g. `increase(oak_security_user_admin_login_failed_total[15m]) > 0`, admin impersonated,
admin pw changed, impersonators granted/revoked).

## Open questions
None.
