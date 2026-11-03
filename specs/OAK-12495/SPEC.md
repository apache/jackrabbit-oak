# Monitor admin user operations

Tracked in [OAK-12495](https://issues.apache.org/jira/browse/OAK-12495).

## Goals

- Expose runtime metrics for security relevant operations on the admin user (configured `adminId`):
  - password based login of the admin (succeeded / failed)
  - impersonation of the admin (succeeded / failed)
  - change of the admin password
  - principals granted / revoked impersonation of the admin (`rep:impersonators`)
- Metrics can be consumed by external alerting (e.g. Prometheus `oak_security_user_admin_*_total`).

## Design Decisions

- Extend the existing `UserMonitor` / `UserMonitorImpl` (OAK-9366) with no-op default methods; no new monitor.
- Use meters (event counts + rates), not gauges or counters.
- Content changes (password, impersonators) are detected in `UserValidator` at commit time, so every write
  path is covered (user management API, XML import, plain Oak `Root`/`Tree` API) and only committed changes
  are counted. Creation of the admin user is not counted as password change.
- Login and impersonation of the admin are recorded in `UserAuthentication`; self-impersonation is skipped.
- No metric carries user ids or principal names.
- Purely additive monitoring with NOOP defaults: no feature toggle.

## Out of Scope

- Admin acting as impersonator of other users.
- Token, guest, pre-authenticated and system logins.
- Other administrative aspects (e.g. administrative-principals configuration); to be addressed separately.
