"""Tests for service versioning (v0.14.7 — PRD-011).

Covers:
- Service.get_versioned_full_name() static method
- Service.full_name property with numeric/string/None versions
- $noVersionPrefix and $noServiceNamePrefix settings
- Registry with versioned services (coexistence, lookup)
- Node INFO packet version/fullName fields
- Remote node fullName reconstruction
"""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from moleculerpy.decorators import action, event
from moleculerpy.node import Node, NodeCatalog
from moleculerpy.registry import Registry
from moleculerpy.service import Service

# ---------------------------------------------------------------------------
# Service.get_versioned_full_name()
# ---------------------------------------------------------------------------


class TestGetVersionedFullName:
    """Tests for the static method that constructs versioned names."""

    def test_numeric_version(self) -> None:
        """Numeric version 2 -> 'v2.users'."""
        assert Service.get_versioned_full_name("users", 2) == "v2.users"

    def test_numeric_version_zero(self) -> None:
        """Numeric version 0 is falsy but not None — should NOT be treated as 'no version'."""
        # In Node.js: version=0 is valid (typeof 0 == "number" -> "v0.users")
        # But Python `0` is falsy. Our implementation checks `is not None`.
        assert Service.get_versioned_full_name("users", 0) == "v0.users"

    def test_string_version_plain(self) -> None:
        """String version 'staging' -> 'staging.users'."""
        assert Service.get_versioned_full_name("users", "staging") == "staging.users"

    def test_string_version_with_v_prefix(self) -> None:
        """String version 'v1' -> 'v1.users' (no double prefix)."""
        assert Service.get_versioned_full_name("users", "v1") == "v1.users"

    def test_no_version(self) -> None:
        """None version -> plain name unchanged."""
        assert Service.get_versioned_full_name("users", None) == "users"

    def test_large_numeric_version(self) -> None:
        """Large version number."""
        assert Service.get_versioned_full_name("api", 100) == "v100.api"

    def test_empty_string_version(self) -> None:
        """Empty string version -> still prepends (edge case)."""
        result = Service.get_versioned_full_name("users", "")
        # Empty string is not None, so it prepends: ".users"
        assert result == ".users"


# ---------------------------------------------------------------------------
# Service.full_name property
# ---------------------------------------------------------------------------


class TestServiceFullName:
    """Tests for the full_name instance attribute."""

    def test_numeric_version_on_class(self) -> None:
        """Service with numeric version attribute."""

        class UserV2(Service):
            name = "users"
            version = 2

        svc = UserV2()
        assert svc.name == "users"
        assert svc.version == 2
        assert svc.full_name == "v2.users"

    def test_string_version_on_class(self) -> None:
        """Service with string version attribute."""

        class AuthBeta(Service):
            name = "auth"
            version = "beta"

        svc = AuthBeta()
        assert svc.full_name == "beta.auth"

    def test_no_version(self) -> None:
        """Service without version -> full_name equals name."""

        class MathService(Service):
            name = "math"

        svc = MathService()
        assert svc.version is None
        assert svc.full_name == "math"

    def test_no_version_prefix_setting(self) -> None:
        """$noVersionPrefix suppresses version prefix."""

        class SuppressedV3(Service):
            name = "users"
            version = 3
            settings = {"$noVersionPrefix": True}

        svc = SuppressedV3()
        assert svc.version == 3
        assert svc.full_name == "users"

    def test_no_version_prefix_false(self) -> None:
        """$noVersionPrefix=False does not suppress."""

        class ExplicitPrefix(Service):
            name = "users"
            version = 5
            settings = {"$noVersionPrefix": False}

        svc = ExplicitPrefix()
        assert svc.full_name == "v5.users"

    def test_version_from_mixin(self) -> None:
        """Version can come from mixin schema."""

        class VersionedMixin:
            version = "v2"

        class MixedService(Service):
            name = "orders"
            mixins = [VersionedMixin]

        svc = MixedService()
        assert svc.version == "v2"
        assert svc.full_name == "v2.orders"


# ---------------------------------------------------------------------------
# Registry with versioned services
# ---------------------------------------------------------------------------


class TestRegistryVersionedServices:
    """Tests for registry handling of versioned services."""

    def test_versioned_service_registration(self) -> None:
        """Versioned service registers actions with full_name prefix."""

        class UserV2(Service):
            name = "users"
            version = 2

            @action()
            async def get(self, ctx):  # type: ignore[no-untyped-def]
                return {"version": 2}

        registry = Registry(node_id="test-node")
        svc = UserV2()
        registry.register(svc)

        # Service stored by full_name
        assert "v2.users" in registry.__services__

        # Action uses full_name prefix
        act = registry.get_action("v2.users.get")
        assert act is not None
        assert act.name == "v2.users.get"

    def test_two_versions_coexist(self) -> None:
        """v1 and v2 of same service coexist in registry."""

        class UserV1(Service):
            name = "users"
            version = 1

            @action()
            async def get(self, ctx):  # type: ignore[no-untyped-def]
                return {"version": 1}

        class UserV2(Service):
            name = "users"
            version = 2

            @action()
            async def get(self, ctx):  # type: ignore[no-untyped-def]
                return {"version": 2}

        registry = Registry(node_id="test-node")
        registry.register(UserV1())
        registry.register(UserV2())

        assert len(registry.__services__) == 2
        assert "v1.users" in registry.__services__
        assert "v2.users" in registry.__services__

        v1 = registry.get_action("v1.users.get")
        v2 = registry.get_action("v2.users.get")
        assert v1 is not None
        assert v2 is not None
        assert v1 is not v2

    def test_unversioned_not_found_by_plain_name(self) -> None:
        """Versioned 'v2.users.get' is not found as 'users.get'."""

        class UserV2(Service):
            name = "users"
            version = 2

            @action()
            async def get(self, ctx):  # type: ignore[no-untyped-def]
                return {}

        registry = Registry(node_id="test-node")
        registry.register(UserV2())

        assert registry.get_action("users.get") is None
        assert registry.get_action("v2.users.get") is not None

    def test_unversioned_service_backwards_compat(self) -> None:
        """Unversioned service works exactly as before."""

        class MathService(Service):
            name = "math"

            @action()
            async def add(self, ctx):  # type: ignore[no-untyped-def]
                return 0

        registry = Registry(node_id="test-node")
        registry.register(MathService())

        assert "math" in registry.__services__
        assert registry.get_action("math.add") is not None

    def test_no_service_name_prefix(self) -> None:
        """$noServiceNamePrefix makes action name = raw name only."""

        class RawService(Service):
            name = "internal"
            settings = {"$noServiceNamePrefix": True}

            @action()
            async def health(self, ctx):  # type: ignore[no-untyped-def]
                return "ok"

        registry = Registry(node_id="test-node")
        registry.register(RawService())

        # Action registered without service prefix
        assert registry.get_action("health") is not None
        assert registry.get_action("internal.health") is None

    def test_get_service_by_full_name(self) -> None:
        """get_service uses full_name as key."""

        class OrderV3(Service):
            name = "orders"
            version = 3

            @action()
            async def list(self, ctx):  # type: ignore[no-untyped-def]
                return []

        registry = Registry(node_id="test-node")
        svc = OrderV3()
        registry.register(svc)

        assert registry.get_service("v3.orders") is svc
        assert registry.get_service("orders") is None


# ---------------------------------------------------------------------------
# Node INFO packet
# ---------------------------------------------------------------------------


class TestNodeInfoVersionField:
    """Tests for version/fullName in INFO packet service definitions."""

    def test_local_node_includes_version(self) -> None:
        """ensure_local_node builds service definitions with version field."""

        class UserV2(Service):
            name = "users"
            version = 2

            @action()
            async def get(self, ctx):  # type: ignore[no-untyped-def]
                return {}

        registry = Registry(node_id="test-node")
        svc = UserV2()
        registry.register(svc)

        catalog = NodeCatalog(registry, Mock(), "test-node")
        catalog.local_node = None
        catalog.ensure_local_node()

        services = catalog.local_node.services
        assert len(services) == 1

        svc_def = services[0]
        assert svc_def["name"] == "users"
        assert svc_def["version"] == 2
        assert svc_def["fullName"] == "v2.users"
        assert "v2.users.get" in svc_def["actions"]

    def test_local_node_unversioned(self) -> None:
        """Unversioned service has version=None and fullName=name."""

        class MathSvc(Service):
            name = "math"

            @action()
            async def add(self, ctx):  # type: ignore[no-untyped-def]
                return 0

        registry = Registry(node_id="test-node")
        registry.register(MathSvc())

        catalog = NodeCatalog(registry, Mock(), "test-node")
        catalog.local_node = None
        catalog.ensure_local_node()

        svc_def = catalog.local_node.services[0]
        assert svc_def["version"] is None
        assert svc_def["fullName"] == "math"
        assert "math.add" in svc_def["actions"]


# ---------------------------------------------------------------------------
# Remote node fullName reconstruction
# ---------------------------------------------------------------------------


class TestRemoteFullNameReconstruction:
    """Tests for reconstructing fullName when missing in remote INFO."""

    def test_reconstruct_fullname_from_version(self) -> None:
        """Remote service without fullName gets it reconstructed."""
        registry = Registry(node_id="local-node")
        catalog = NodeCatalog(registry, Mock(), "local-node")

        # Simulate remote node with services that lack fullName
        remote_node = Node("remote-1")
        remote_node.services = [
            {
                "name": "users",
                "version": 2,
                "actions": {"v2.users.get": {"name": "v2.users.get", "rawName": "get"}},
                "events": {},
            }
        ]

        catalog._register_node_endpoints("remote-1", remote_node)

        # fullName should have been reconstructed
        assert remote_node.services[0]["fullName"] == "v2.users"

    def test_existing_fullname_not_overwritten(self) -> None:
        """Remote service with existing fullName is not changed."""
        registry = Registry(node_id="local-node")
        catalog = NodeCatalog(registry, Mock(), "local-node")

        remote_node = Node("remote-1")
        remote_node.services = [
            {
                "name": "users",
                "version": 2,
                "fullName": "v2.users",
                "actions": {},
                "events": {},
            }
        ]

        catalog._register_node_endpoints("remote-1", remote_node)
        assert remote_node.services[0]["fullName"] == "v2.users"

    def test_reconstruct_without_version(self) -> None:
        """Remote service without version -> fullName = name."""
        registry = Registry(node_id="local-node")
        catalog = NodeCatalog(registry, Mock(), "local-node")

        remote_node = Node("remote-1")
        remote_node.services = [
            {
                "name": "math",
                "actions": {},
                "events": {},
            }
        ]

        catalog._register_node_endpoints("remote-1", remote_node)
        assert remote_node.services[0]["fullName"] == "math"


# ---------------------------------------------------------------------------
# Settings propagation fix (bugfix found during versioning work)
# ---------------------------------------------------------------------------


class TestSettingsPropagationFix:
    """Tests for class-level settings propagation without mixins.

    This bug was discovered during service versioning implementation:
    settings defined on a Service subclass without mixins were not
    propagated to instances.
    """

    def test_class_settings_without_mixins(self) -> None:
        """Settings defined on class without mixins are available on instance."""

        class ConfiguredService(Service):
            name = "configured"
            settings = {"timeout": 5000, "retries": 3}

        svc = ConfiguredService()
        assert svc.settings == {"timeout": 5000, "retries": 3}

    def test_class_metadata_without_mixins(self) -> None:
        """Metadata defined on class without mixins are available on instance."""

        class TaggedService(Service):
            name = "tagged"
            metadata = {"region": "us-east-1"}

        svc = TaggedService()
        assert svc.metadata == {"region": "us-east-1"}

    def test_class_dependencies_without_mixins(self) -> None:
        """Dependencies defined on class without mixins are available on instance."""

        class DependentService(Service):
            name = "dependent"
            dependencies = ["auth", "config"]

        svc = DependentService()
        assert svc.dependencies == ["auth", "config"]
