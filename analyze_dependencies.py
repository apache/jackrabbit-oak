#!/usr/bin/env python3
"""
Analyze Maven dependencies across the Jackrabbit Oak project to identify redundancies.
"""

import os
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

# Maven namespace
NS = {'mvn': 'http://maven.apache.org/POM/4.0.0'}

def parse_pom(pom_path):
    """Parse a pom.xml file and extract dependencies."""
    try:
        tree = ET.parse(pom_path)
        root = tree.getroot()

        # Get artifactId for identification
        artifact_id_elem = root.find('mvn:artifactId', NS)
        if artifact_id_elem is None:
            artifact_id_elem = root.find('artifactId')
        artifact_id = artifact_id_elem.text if artifact_id_elem is not None else "unknown"

        dependencies = []
        dep_management = []

        # Extract regular dependencies
        deps_section = root.find('mvn:dependencies', NS)
        if deps_section is None:
            deps_section = root.find('dependencies')

        if deps_section is not None:
            for dep in deps_section.findall('mvn:dependency', NS) or deps_section.findall('dependency'):
                dep_info = extract_dependency_info(dep)
                if dep_info:
                    dependencies.append(dep_info)

        # Extract dependencyManagement
        dep_mgmt_section = root.find('mvn:dependencyManagement', NS)
        if dep_mgmt_section is None:
            dep_mgmt_section = root.find('dependencyManagement')

        if dep_mgmt_section is not None:
            deps_in_mgmt = dep_mgmt_section.find('mvn:dependencies', NS)
            if deps_in_mgmt is None:
                deps_in_mgmt = dep_mgmt_section.find('dependencies')

            if deps_in_mgmt is not None:
                for dep in deps_in_mgmt.findall('mvn:dependency', NS) or deps_in_mgmt.findall('dependency'):
                    dep_info = extract_dependency_info(dep)
                    if dep_info:
                        dep_management.append(dep_info)

        return artifact_id, dependencies, dep_management
    except Exception as e:
        print(f"Error parsing {pom_path}: {e}")
        return None, [], []

def extract_dependency_info(dep_elem):
    """Extract groupId, artifactId, version, scope from a dependency element."""
    group_id = None
    artifact_id = None
    version = None
    scope = None
    optional = None

    # Try with namespace first
    for child in dep_elem:
        tag = child.tag.split('}')[-1]  # Remove namespace prefix
        if tag == 'groupId':
            group_id = child.text
        elif tag == 'artifactId':
            artifact_id = child.text
        elif tag == 'version':
            version = child.text
        elif tag == 'scope':
            scope = child.text
        elif tag == 'optional':
            optional = child.text

    if group_id and artifact_id:
        return {
            'groupId': group_id,
            'artifactId': artifact_id,
            'version': version,
            'scope': scope,
            'optional': optional,
            'key': f"{group_id}:{artifact_id}"
        }
    return None

def find_all_poms(root_dir):
    """Find all pom.xml files in the project."""
    pom_files = []
    for root, dirs, files in os.walk(root_dir):
        if 'pom.xml' in files:
            pom_files.append(os.path.join(root, 'pom.xml'))
    return pom_files

def analyze_redundancies(project_root):
    """Analyze all pom files for redundant dependencies."""
    pom_files = find_all_poms(project_root)

    # Data structures
    module_deps = {}  # module -> list of dependencies
    module_dep_mgmt = {}  # module -> list of dependency management
    dep_frequency = defaultdict(int)  # dependency key -> count
    dep_versions = defaultdict(set)  # dependency key -> set of versions
    dep_locations = defaultdict(list)  # dependency key -> list of (module, version)

    parent_dep_mgmt = None

    print(f"Found {len(pom_files)} pom.xml files")
    print("=" * 80)

    # Parse all poms
    for pom_file in sorted(pom_files):
        artifact_id, dependencies, dep_management = parse_pom(pom_file)

        if artifact_id == "oak-parent":
            parent_dep_mgmt = {d['key']: d for d in dep_management}

        module_deps[artifact_id] = dependencies
        module_dep_mgmt[artifact_id] = dep_management

        # Track dependency usage
        for dep in dependencies:
            dep_key = dep['key']
            dep_frequency[dep_key] += 1
            if dep['version']:
                dep_versions[dep_key].add(dep['version'])
            dep_locations[dep_key].append((artifact_id, dep.get('version', 'managed')))

    # Generate report
    report_lines = []
    report_lines.append("# Redundant Dependencies Analysis Report")
    report_lines.append("")
    report_lines.append("Generated: 2026-03-04")
    report_lines.append("")
    report_lines.append("## Executive Summary")
    report_lines.append("")
    report_lines.append(f"Total modules analyzed: {len(module_deps)}")
    report_lines.append(f"Unique dependencies: {len(dep_frequency)}")
    report_lines.append("")

    # Finding 1: Dependencies with multiple versions
    report_lines.append("## 1. Dependencies with Multiple Versions")
    report_lines.append("")
    report_lines.append("These dependencies are declared with different versions across modules,")
    report_lines.append("which can lead to classpath conflicts and unpredictable behavior.")
    report_lines.append("")

    multi_version_deps = {k: v for k, v in dep_versions.items() if len(v) > 1}
    if multi_version_deps:
        for dep_key, versions in sorted(multi_version_deps.items()):
            report_lines.append(f"### {dep_key}")
            report_lines.append(f"Versions found: {', '.join(sorted(v for v in versions if v))}")
            report_lines.append("")
            report_lines.append("Locations:")
            for module, version in dep_locations[dep_key]:
                report_lines.append(f"- {module}: {version}")
            report_lines.append("")
    else:
        report_lines.append("✓ No dependencies with version conflicts found")
        report_lines.append("")

    # Finding 2: Widely used dependencies that could be centralized
    report_lines.append("## 2. Frequently Declared Dependencies")
    report_lines.append("")
    report_lines.append("These dependencies are declared in many modules. Consider adding them to")
    report_lines.append("dependencyManagement in oak-parent to centralize version control.")
    report_lines.append("")

    frequent_deps = {k: v for k, v in dep_frequency.items() if v >= 5}
    if frequent_deps:
        for dep_key, count in sorted(frequent_deps.items(), key=lambda x: x[1], reverse=True):
            if parent_dep_mgmt and dep_key in parent_dep_mgmt:
                continue  # Already in parent management
            report_lines.append(f"### {dep_key}")
            report_lines.append(f"Used in {count} modules")
            report_lines.append("")
            modules = [m for m, v in dep_locations[dep_key]]
            report_lines.append(f"Modules: {', '.join(sorted(modules)[:5])}")
            if len(modules) > 5:
                report_lines.append(f"... and {len(modules) - 5} more")
            report_lines.append("")
    else:
        report_lines.append("✓ No frequently declared dependencies that need centralization")
        report_lines.append("")

    # Finding 3: Dependencies that specify version when it's managed
    report_lines.append("## 3. Redundant Version Declarations")
    report_lines.append("")
    report_lines.append("These dependencies specify a version explicitly even though the version")
    report_lines.append("is already managed in oak-parent dependencyManagement. The version")
    report_lines.append("declaration can be removed to rely on the parent version.")
    report_lines.append("")

    redundant_versions = []
    if parent_dep_mgmt:
        for module, deps in module_deps.items():
            if module == "oak-parent":
                continue
            for dep in deps:
                dep_key = dep['key']
                if dep_key in parent_dep_mgmt and dep.get('version'):
                    # Check if it's not a property reference
                    version = dep['version']
                    if version and not version.startswith('${'):
                        redundant_versions.append((module, dep_key, version))

    if redundant_versions:
        for module, dep_key, version in sorted(redundant_versions):
            report_lines.append(f"- {module}: {dep_key} (specifies version {version})")
        report_lines.append("")
    else:
        report_lines.append("✓ No redundant version declarations found")
        report_lines.append("")

    # Finding 4: Test-scoped dependencies
    report_lines.append("## 4. Test Dependencies Analysis")
    report_lines.append("")
    report_lines.append("Common test dependencies used across modules:")
    report_lines.append("")

    test_deps = defaultdict(int)
    for module, deps in module_deps.items():
        for dep in deps:
            if dep.get('scope') == 'test':
                test_deps[dep['key']] += 1

    common_test_deps = {k: v for k, v in test_deps.items() if v >= 5}
    if common_test_deps:
        for dep_key, count in sorted(common_test_deps.items(), key=lambda x: x[1], reverse=True):
            report_lines.append(f"- {dep_key}: used in {count} modules")
        report_lines.append("")
    else:
        report_lines.append("✓ Test dependencies appear well-managed")
        report_lines.append("")

    # Recommendations
    report_lines.append("## Recommendations")
    report_lines.append("")
    report_lines.append("1. **Version Conflicts**: Review dependencies with multiple versions and standardize")
    report_lines.append("   on a single version across all modules. Add to parent dependencyManagement.")
    report_lines.append("")
    report_lines.append("2. **Centralize Version Management**: Move frequently used dependencies to")
    report_lines.append("   oak-parent dependencyManagement to ensure consistent versions.")
    report_lines.append("")
    report_lines.append("3. **Remove Redundant Versions**: Remove explicit version declarations from")
    report_lines.append("   module pom.xml files when the version is already managed in parent.")
    report_lines.append("")
    report_lines.append("4. **Test Dependencies**: Consider standardizing test dependency versions")
    report_lines.append("   in parent dependencyManagement for consistency.")
    report_lines.append("")

    return "\n".join(report_lines)

if __name__ == "__main__":
    project_root = "/workspace/jackrabbit-oak"
    report = analyze_redundancies(project_root)

    # Write report to file
    with open("/workspace/jackrabbit-oak/DEPENDENCY_ANALYSIS_REPORT.md", "w") as f:
        f.write(report)

    print(report)
