# Wiring and Verification

## STEP 11 — Wire the config into its usage class

Ask the user:
> **"Which class do you want to use `{utilsMethod}()` in? (e.g. `DocumentNodeStoreBranch`, `DocumentNodeStore`, etc.)"**

Wait for the answer before proceeding.

### 11a. `DocumentNodeStore.java` — read config from builder, store as field

Read `DocumentNodeStore.java` first to understand existing field and constructor patterns.

Add the field declaration near similar fields:
```java
private final boolean {fieldName};
```

In the constructor `DocumentNodeStore(DocumentNodeStoreBuilder<?> builder)`, alongside similar assignments:
```java
this.{fieldName} = {utilsMethod}(builder);
```

Add the static import alongside other `Utils.*` imports at the top:
```java
import static ...util.Utils.{utilsMethod};
```

### 11b. Target class — pass the value through

If the target class is NOT `DocumentNodeStore` itself (e.g. `DocumentNodeStoreBranch`):

1. Find where `DocumentNodeStore` instantiates the target class and pass the field as an extra argument.
2. In the target class constructor, accept and assign it:
   ```java
   private final boolean {fieldName};
   // in constructor:
   this.{fieldName} = {fieldName};
   ```
3. Use `this.{fieldName}` wherever the logic needs it.

If the target IS `DocumentNodeStore`, skip 11b — the field is already there from 11a.

### 11c. Update ALL existing constructor call sites (STRICTLY ENFORCED)

When adding a new parameter to a constructor:
- **NEVER add a backward-compatibility overload**
- Grep all call sites (prod + test) and update every one to pass the default value

```bash
grep -rn "new TargetClass(" oak-store-document/src/
```

For each call site, add the default value (e.g. `false` for boolean, `0` for int/long, `null` for String).

---

## STEP 12 — Compile and run tests

```bash
cd "$(git rev-parse --show-toplevel)"
mvn compile -pl oak-store-document -q 2>&1 | tail -20
```

If compilation fails, diagnose and fix, then retry.

```bash
mvn test -pl oak-store-document -am \
  -Dtest="DocumentNodeStoreServiceConfigurationTest,MongoDocumentNodeStoreBuilderTest,RDBDocumentNodeStoreBuilderTest,UtilsTest" \
  2>&1 | tail -40
```

Fix any failures and re-run until all pass.

> **Note**: Pre-existing unrelated compile errors (e.g. `NodeCache.java`) should be ignored — verify they exist on trunk before treating them as caused by your changes.
