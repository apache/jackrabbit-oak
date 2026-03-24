# Test Patterns

> **Assertion style rule**: Always match the existing import style in each file — do NOT add new imports to impose a style. See IMPORTANT CONSTRAINTS in SKILL.md.

---

## STEP 7 — DocumentNodeStoreServiceConfigurationTest.java

Uses `Assert.assertEquals(...)` (non-static). Add the import alphabetically:
```java
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.{DEFAULT_CONST};
```

**Test 1**: Add assertion inside the existing `defaultValues()` test:
```java
Assert.assertEquals({DEFAULT_CONST}, config.{configName}());
```

**Test 2**: New test method:
```java
@Test
public void {configName}Enabled() throws Exception {
    {type} value = {nonDefaultValue};
    addConfigurationEntry(preset, "{configName}", value);
    Configuration config = createConfiguration();
    Assert.assertEquals(value, config.{configName}());
}
```

---

## STEP 8 — MongoDocumentNodeStoreBuilderTest.java

Uses static imports (`assertFalse`, `assertNull`). Always add:
```java
@Test
public void {getterName}Disabled() {
    MongoDocumentNodeStoreBuilder builder = new MongoDocumentNodeStoreBuilder();
    assertFalse(builder.{getterName}());
}
```

When `featureToggle=true`, also add:
```java
@Test
public void {getterName}FeatureToggleEnabled() {
    MongoDocumentNodeStoreBuilder builder = new MongoDocumentNodeStoreBuilder();
    assertNull(builder.get{PascalCase(featureField)}());
}
```

---

## STEP 9 — RDBDocumentNodeStoreBuilderTest.java

Uses static imports (`assertFalse`, `assertNull`, `mock`, `when`).

**scope=both** — verify base class defaults work for RDB too:
```java
@Test
public void {getterName}Disabled() {
    RDBDocumentNodeStoreBuilder builder = new RDBDocumentNodeStoreBuilder();
    assertFalse(builder.{getterName}());
}
```
When `featureToggle=true`, also add:
```java
@Test
public void {getterName}FeatureToggleEnabled() {
    RDBDocumentNodeStoreBuilder builder = new RDBDocumentNodeStoreBuilder();
    assertNull(builder.get{PascalCase(featureField)}());
}
```

**scope=mongo-only** — verify RDB override ignores the value:
```java
@Test
public void {getterName}Disabled() {
    RDBDocumentNodeStoreBuilder builder = new RDBDocumentNodeStoreBuilder();
    builder.{setterName}({nonDefaultValue});
    assertFalse(builder.{getterName}());
}
```
When `featureToggle=true`, also add:
```java
@Test
public void {getterName}FeatureToggleDisabled() {
    RDBDocumentNodeStoreBuilder builder = new RDBDocumentNodeStoreBuilder();
    Feature feature = mock(Feature.class);
    when(feature.isEnabled()).thenReturn(true);
    builder.set{PascalCase(featureField)}(feature);
    assertNull(builder.get{PascalCase(featureField)}());
}
```

---

## STEP 10 — UtilsTest.java

Uses static imports for assertions. Check the file's existing Mockito import style and match it.

**Always add** (regardless of `featureToggle`):
```java
@Test
public void {getterName}DefaultValue() {
    boolean result = Utils.{utilsMethod}(DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder());
    assertFalse("Feature is disabled by default", result);
}

@Test
public void {getterName}EnabledViaConfiguration() {
    DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder();
    builder.{setterName}(true);
    assertTrue("Feature is enabled via configuration", Utils.{utilsMethod}(builder));
}
```

**featureToggle=true**, also add:
```java
@Test
public void {getterName}ExplicitlyDisabled() {
    DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder();
    builder.{setterName}(false);
    Feature feature = Mockito.mock(Feature.class);
    Mockito.when(feature.isEnabled()).thenReturn(false);
    builder.set{PascalCase(featureField)}(feature);
    assertFalse("Feature is disabled explicitly", Utils.{utilsMethod}(builder));
}

@Test
public void {getterName}EnabledViaFeatureToggle() {
    DocumentNodeStoreBuilder<?> builder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder();
    builder.{setterName}(false);
    Feature feature = Mockito.mock(Feature.class);
    Mockito.when(feature.isEnabled()).thenReturn(true);
    builder.set{PascalCase(featureField)}(feature);
    assertTrue("Feature is enabled via feature toggle", Utils.{utilsMethod}(builder));
}
```

**scope=mongo-only**, also add:
```java
@Test
public void {getterName}DisabledForRDB() {
    DocumentNodeStoreBuilder<?> builder = RDBDocumentNodeStoreBuilder.newRDBDocumentNodeStoreBuilder();
    builder.{setterName}(true);
    assertFalse("Feature is disabled for RDB", Utils.{utilsMethod}(builder));
}
```

**Test count summary:**
| featureToggle | scope | UtilsTest count |
|---|---|---|
| false | both | 2 |
| false | mongo-only | 3 |
| true | both | 4 |
| true | mongo-only | 5 |
