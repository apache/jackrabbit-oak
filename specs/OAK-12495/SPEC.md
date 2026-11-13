# Monitor admin user operations

Tracked in [OAK-12495](https://issues.apache.org/jira/browse/OAK-12495).

## Goals

- Expose runtime metrics for security relevant operations on the admin user (configured `adminId`):
  - password based login of the admin (succeeded / failed)
  - impersonation of the admin (succeeded / failed)
  - addition (none before) / change of the admin password
  - principals granted / revoked impersonation of the admin (`rep:impersonators`)
- Metrics can be consumed by external alerting (e.g. Prometheus `oak_security_user_admin_*_total`).

## Design Decisions

- Extend the existing `UserMonitor` / `UserMonitorImpl` (OAK-9366) with no-op default methods; no new monitor.
- Use meters (event counts + rates), not gauges or counters.
- Content changes (password, impersonators) are detected in `UserValidator` during commit validation, so every
  write path is covered (user management API, XML import, plain Oak `Root`/`Tree` API) and changes rejected by
  the property checks in `UserValidator` (e.g. plain-text password change, password removal) are not counted. Creation of the admin user is not counted (neither password nor impersonators).
- The content-change metrics are best-effort: they count changes observed during commit validation. Subsequent
  validation or merge failures may still leave them incremented, and commit retries may overcount. They support
  alerting and investigation; they do not prove persistence and do not replace an audit trail. This does not apply
  to the login/impersonation metrics, which record authentication attempts.
- Adding a password to an existing admin user without one (e.g. `omitAdminPw`) is distinguished from changing
  an existing password, as it enables password login for the admin.
- Login and impersonation of the admin are recorded in `UserAuthentication`, including admin self-impersonation.
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
