# Monitor admin user operations

Tracked in [OAK-12495](https://issues.apache.org/jira/browse/OAK-12495).

## Goals

- Report security relevant operations on existing users to `UserMonitor` through a generic API, where each call
  indicates whether the user is the admin user (configured `adminId`):
  - password based login (succeeded / failed)
  - impersonation of the user (succeeded / failed)
  - addition (none before) / change of the password
  - principals granted / revoked impersonation of the user (`rep:impersonators`)
- The default `UserMonitorImpl` exposes runtime metrics for these operations on the admin user only.
- Metrics can be consumed by external alerting (e.g. Prometheus `oak_security_user_admin_*_total`).

## Design Decisions

- Extend the existing `UserMonitor` / `UserMonitorImpl` (OAK-9366) with no-op default methods; no new monitor.
- Use meters (event counts + rates), not gauges or counters.
- Content changes (password, impersonators) are detected in `UserValidator` during commit validation, so every
  write path is covered (user management API, XML import, plain Oak `Root`/`Tree` API) and changes rejected by
  the property checks in `UserValidator` (e.g. plain-text password change, password removal) are not reported.
  Creation of a user (incl. the admin) is not reported (neither password nor impersonators); this includes
  converting an existing non-user node (e.g. a folder) into a user.
- The content-change metrics are best-effort: they count changes observed during commit validation. Subsequent
  validation or merge failures may still leave them incremented, and commit retries may overcount. They support
  alerting and investigation; they do not prove persistence and do not replace an audit trail. This does not apply
  to the login/impersonation metrics, which record authentication attempts.
- Adding a password to an existing user without one (e.g. admin with `omitAdminPw`) is distinguished from changing
  an existing password, as it enables password login for the user.
- Login and impersonation are reported from `UserAuthentication`, including admin self-impersonation. `success`
  reflects the decision of `UserAuthentication`, not the outcome of the overall JAAS login. Attempts against disabled
  users are reported as failed (the admin user cannot be disabled). Unknown ids and groups are not reported, as they
  do not resolve to a user. Guest and pre-authenticated logins are not reported.
- The `UserMonitor` API is generic rather than admin specific: `login(success, isAdmin)`,
  `impersonated(success, isAdmin)`, `passwordChanged(added, isAdmin)` and
  `impersonatorsChanged(granted, revoked, isAdmin)` are called for every existing user. `UserMonitorImpl` records
  only calls with `isAdmin=true`, so monitoring can be extended to other users without changing the API.
  `isAdmin` refers to the user with the configured `adminId`, not to any principal with administrative privileges.
- No metric carries user ids or principal names.
- Purely additive monitoring with NOOP defaults: no feature toggle.

## Out of Scope

- Admin acting as impersonator of other users.
- Token, guest, pre-authenticated and system logins with `AdminPrincipal` / `SystemPrincipal` and Oak system sessions.
- Custom `UserAuthenticationFactory` implementations (logins not handled by the default `UserAuthentication` are not
  recorded).
- Changes bypassing commit hooks (e.g. offline tooling writing directly to the NodeStore).
- Capturing the user agent (or other request details) of the caller.
- Rejecting a plain-text `rep:password` added to an existing user (see PLAN.md, section 4.4).
- Other administrative aspects (e.g. administrative-principals configuration); to be addressed separately.
