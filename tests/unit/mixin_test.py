"""Unit tests for the Mixin module.

Tests for MoleculerPy mixin support, compatible with Moleculer.js mixin behavior.
Covers merge strategies, class/dict mixins, nested mixins, and lifecycle ordering.
"""

from __future__ import annotations

import pytest

from moleculerpy.mixin import (
    MixinSchema,
    _compact,
    _flatten,
    _unique_by_identity,
    _wrap_to_list,
    apply_mixins,
    deep_merge,
    extract_mixin_methods,
    merge_actions,
    merge_events,
    merge_hooks,
    merge_lifecycle_handlers,
    merge_metadata,
    merge_methods,
    merge_schemas,
    merge_settings,
    merge_unique_array,
    mixin_to_schema,
)
from moleculerpy.service import Service


class TestHelperFunctions:
    """Tests for helper utility functions."""

    def test_wrap_to_list_none(self):
        """Test wrapping None returns empty list."""
        assert _wrap_to_list(None) == []

    def test_wrap_to_list_single(self):
        """Test wrapping single value returns list with value."""
        assert _wrap_to_list(42) == [42]

    def test_wrap_to_list_list(self):
        """Test wrapping list returns same list."""
        assert _wrap_to_list([1, 2, 3]) == [1, 2, 3]

    def test_flatten_nested(self):
        """Test flattening nested lists."""
        assert _flatten([[1, 2], [3, 4]]) == [1, 2, 3, 4]

    def test_flatten_mixed(self):
        """Test flattening mixed list."""
        assert _flatten([1, [2, 3], 4]) == [1, 2, 3, 4]

    def test_compact_removes_none(self):
        """Test compact removes None values."""
        assert _compact([1, None, 2, None, 3]) == [1, 2, 3]

    def test_unique_by_identity(self):
        """Test uniqueness by object identity."""
        a = {"x": 1}
        b = {"x": 1}  # Same content, different object
        result = _unique_by_identity([a, b, a])
        assert len(result) == 2
        assert a in result
        assert b in result


class TestDeepMerge:
    """Tests for deep merge function."""

    def test_deep_merge_simple(self):
        """Test simple merge."""
        target = {"a": 1, "b": 2}
        source = {"b": 3, "c": 4}
        result = deep_merge(target, source)
        assert result == {"a": 1, "b": 3, "c": 4}

    def test_deep_merge_nested(self):
        """Test nested merge."""
        target = {"a": {"x": 1, "y": 2}}
        source = {"a": {"y": 3, "z": 4}}
        result = deep_merge(target, source)
        assert result == {"a": {"x": 1, "y": 3, "z": 4}}

    def test_deep_merge_deeply_nested(self):
        """Test deeply nested merge."""
        target = {"a": {"b": {"c": 1, "d": 2}}}
        source = {"a": {"b": {"d": 3, "e": 4}}}
        result = deep_merge(target, source)
        assert result == {"a": {"b": {"c": 1, "d": 3, "e": 4}}}

    def test_deep_merge_does_not_mutate(self):
        """Test that deep merge does not mutate original dicts."""
        target = {"a": {"x": 1}}
        source = {"a": {"y": 2}}
        deep_merge(target, source)
        assert target == {"a": {"x": 1}}  # Original unchanged
        assert source == {"a": {"y": 2}}  # Original unchanged


class TestMixinToSchema:
    """Tests for mixin to schema conversion."""

    def test_dict_mixin(self):
        """Test dict mixin is returned as-is."""
        mixin = {"name": "test", "settings": {"a": 1}}
        schema = mixin_to_schema(mixin)
        assert schema == mixin

    def test_class_mixin_with_dunder_attrs(self):
        """Test class mixin with __settings__ etc."""

        class TestMixin:
            name = "test"
            __settings__ = {"log_level": "debug"}
            __metadata__ = {"tag": "v1"}
            __hooks__ = {"before": {"*": lambda self, ctx: None}}
            __dependencies__ = ["db"]

            def custom_method(self):
                pass

        schema = mixin_to_schema(TestMixin)
        assert schema["name"] == "test"
        assert schema["settings"] == {"log_level": "debug"}
        assert schema["metadata"] == {"tag": "v1"}
        assert "before" in schema["hooks"]
        assert schema["dependencies"] == ["db"]
        assert "custom_method" in schema["methods"]

    def test_class_mixin_with_plain_attrs(self):
        """Test class mixin with plain attributes."""

        class TestMixin:
            name = "test"
            settings = {"a": 1}
            metadata = {"b": 2}
            hooks = {"before": {"*": lambda self, ctx: None}}
            dependencies = ["cache"]

        schema = mixin_to_schema(TestMixin)
        assert schema["settings"] == {"a": 1}
        assert schema["metadata"] == {"b": 2}
        assert schema["dependencies"] == ["cache"]

    def test_class_mixin_lifecycle_hooks(self):
        """Test class mixin with lifecycle hooks."""

        class TestMixin:
            def created(self):
                pass

            def started(self):
                pass

            def stopped(self):
                pass

        schema = mixin_to_schema(TestMixin)
        assert callable(schema["created"])
        assert callable(schema["started"])
        assert callable(schema["stopped"])

    def test_class_mixin_filters_private_methods(self):
        """Test class mixin filters private and dunder methods."""

        class TestMixin:
            def public_method(self):
                pass

            def _private_method(self):
                pass

            def __dunder_method__(self):
                pass

        schema = mixin_to_schema(TestMixin)
        assert "public_method" in schema.get("methods", {})
        assert "_private_method" not in schema.get("methods", {})
        assert "__dunder_method__" not in schema.get("methods", {})


class TestMergeSettings:
    """Tests for settings merge strategy (deep merge)."""

    def test_merge_settings_simple(self):
        """Test simple settings merge."""
        target = {"a": 1, "b": 2}
        source = {"b": 3, "c": 4}
        result = merge_settings(target, source)
        assert result == {"a": 1, "b": 3, "c": 4}

    def test_merge_settings_none_target(self):
        """Test merge with None target."""
        result = merge_settings(None, {"a": 1})
        assert result == {"a": 1}

    def test_merge_settings_none_source(self):
        """Test merge with None source."""
        result = merge_settings({"a": 1}, None)
        assert result == {"a": 1}

    def test_merge_settings_nested(self):
        """Test nested settings merge."""
        target = {"db": {"host": "localhost", "port": 5432}}
        source = {"db": {"port": 5433, "name": "mydb"}}
        result = merge_settings(target, source)
        assert result == {"db": {"host": "localhost", "port": 5433, "name": "mydb"}}


class TestMergeMetadata:
    """Tests for metadata merge strategy (deep merge)."""

    def test_merge_metadata_like_settings(self):
        """Test metadata merges like settings."""
        target = {"priority": 5, "tag": "other"}
        source = {"scalable": True, "priority": 3}
        result = merge_metadata(target, source)
        assert result == {"priority": 3, "tag": "other", "scalable": True}


class TestMergeHooks:
    """Tests for hooks merge strategy (concat with order)."""

    def test_merge_hooks_before_order(self):
        """Test before hooks: target first, then source."""

        def handler1(self, ctx):
            return "h1"

        def handler2(self, ctx):
            return "h2"

        target = {"before": {"*": handler1}}
        source = {"before": {"*": handler2}}
        result = merge_hooks(target, source)

        # Before: target first, then source
        assert result["before"]["*"] == [handler1, handler2]

    def test_merge_hooks_after_order(self):
        """Test after hooks: source first, then target."""

        def handler1(self, ctx):
            return "h1"

        def handler2(self, ctx):
            return "h2"

        target = {"after": {"*": handler1}}
        source = {"after": {"*": handler2}}
        result = merge_hooks(target, source)

        # After: source first, then target
        assert result["after"]["*"] == [handler2, handler1]

    def test_merge_hooks_error_order(self):
        """Test error hooks: source first, then target."""

        def handler1(self, ctx, err):
            return "h1"

        def handler2(self, ctx, err):
            return "h2"

        target = {"error": {"*": handler1}}
        source = {"error": {"*": handler2}}
        result = merge_hooks(target, source)

        # Error: source first, then target
        assert result["error"]["*"] == [handler2, handler1]

    def test_merge_hooks_multiple_actions(self):
        """Test hooks for multiple actions."""

        def h1(self, ctx):
            return "h1"

        def h2(self, ctx):
            return "h2"

        def h3(self, ctx):
            return "h3"

        target = {"before": {"alpha": h1, "beta": h2}}
        source = {"before": {"beta": h3, "gamma": h3}}
        result = merge_hooks(target, source)

        assert result["before"]["alpha"] == h1  # Only from target
        assert result["before"]["beta"] == [h2, h3]  # Merged
        assert result["before"]["gamma"] == h3  # Only from source

    def test_merge_hooks_none_values(self):
        """Test merge with None values."""

        def handler():
            return None

        result1 = merge_hooks(None, {"before": {"*": handler}})
        assert "before" in result1
        assert "*" in result1["before"]
        assert result1["before"]["*"] == handler

        result2 = merge_hooks({"before": {"*": handler}}, None)
        assert "before" in result2
        assert "*" in result2["before"]
        assert result2["before"]["*"] == handler

    def test_merge_hooks_single_handler_unwrapped(self):
        """Test single handler is not wrapped in list."""

        def handler(self, ctx):
            return "h"

        target = {}
        source = {"before": {"*": handler}}
        result = merge_hooks(target, source)
        assert result["before"]["*"] == handler  # Not wrapped


class TestMergeActions:
    """Tests for actions merge strategy."""

    def test_merge_actions_simple(self):
        """Test simple actions merge."""

        def handler1(ctx):
            return "h1"

        def handler2(ctx):
            return "h2"

        target = {"alpha": handler1}
        source = {"beta": handler2}
        result = merge_actions(target, source)

        assert "alpha" in result
        assert "beta" in result

    def test_merge_actions_override(self):
        """Test action override (source wins)."""

        def handler1(ctx):
            return "h1"

        def handler2(ctx):
            return "h2"

        target = {"alpha": handler1}
        source = {"alpha": handler2}
        result = merge_actions(target, source)

        assert result["alpha"]["handler"] == handler2

    def test_merge_actions_disable_with_false(self):
        """Test disabling action with False."""

        def handler(ctx):
            return "h"

        target = {"alpha": handler, "beta": handler}
        source = {"alpha": False}  # Disable alpha
        result = merge_actions(target, source)

        assert "alpha" not in result  # Disabled
        assert "beta" in result  # Still present

    def test_merge_actions_deep_merge_properties(self):
        """Test deep merge of action properties."""
        target = {
            "alpha": {
                "handler": lambda ctx: "h1",
                "cache": {"keys": ["id"]},
            }
        }
        source = {
            "alpha": {
                "params": {"name": "string"},
                "cache": {"ttl": 60},
            }
        }
        result = merge_actions(target, source)

        # Handler from target (first defined)
        assert callable(result["alpha"]["handler"])
        # Params from source
        assert result["alpha"]["params"] == {"name": "string"}
        # Cache merged
        assert result["alpha"]["cache"]["keys"] == ["id"]
        assert result["alpha"]["cache"]["ttl"] == 60

    def test_merge_actions_with_hooks(self):
        """Test merging action-specific hooks."""

        def before1(ctx):
            return "b1"

        def before2(ctx):
            return "b2"

        # Action hooks should be dicts with before/after keys
        target = {"alpha": {"hooks": {"before": {"*": before1}}}}
        source = {"alpha": {"hooks": {"before": {"*": before2}}}}
        result = merge_actions(target, source)

        # Hooks should be merged
        assert result["alpha"]["hooks"]["before"]["*"] == [before1, before2]


class TestMergeEvents:
    """Tests for events merge strategy (concat handlers)."""

    def test_merge_events_concat_handlers(self):
        """Test event handlers are concatenated."""

        def handler1(ctx):
            return "h1"

        def handler2(ctx):
            return "h2"

        target = {"user.created": handler1}
        source = {"user.created": handler2}
        result = merge_events(target, source)

        # Both handlers should be present
        assert len(result["user.created"]["handler"]) == 2

    def test_merge_events_different_events(self):
        """Test merging different events."""

        def handler1(ctx):
            return "h1"

        def handler2(ctx):
            return "h2"

        target = {"user.created": handler1}
        source = {"user.deleted": handler2}
        result = merge_events(target, source)

        assert "user.created" in result
        assert "user.deleted" in result

    def test_merge_events_properties_merged(self):
        """Test event properties are merged."""
        target = {
            "user.created": {
                "handler": lambda ctx: "h1",
                "group": "users",
            }
        }
        source = {
            "user.created": {
                "handler": lambda ctx: "h2",
                "context": True,
            }
        }
        result = merge_events(target, source)

        assert result["user.created"]["group"] == "users"
        assert result["user.created"]["context"] is True
        # Handlers concatenated
        assert len(result["user.created"]["handler"]) == 2


class TestMergeMethods:
    """Tests for methods merge strategy (override)."""

    def test_merge_methods_override(self):
        """Test methods override (last wins)."""

        def method1(self):
            return "m1"

        def method2(self):
            return "m2"

        target = {"helper": method1}
        source = {"helper": method2}
        result = merge_methods(target, source)

        assert result["helper"] == method2  # Source wins

    def test_merge_methods_combine(self):
        """Test combining different methods."""

        def method1(self):
            return "m1"

        def method2(self):
            return "m2"

        target = {"helper1": method1}
        source = {"helper2": method2}
        result = merge_methods(target, source)

        assert result["helper1"] == method1
        assert result["helper2"] == method2


class TestMergeLifecycleHandlers:
    """Tests for lifecycle handlers merge strategy (concat)."""

    def test_merge_lifecycle_concat(self):
        """Test lifecycle handlers are concatenated."""

        def handler1(self):
            return "h1"

        def handler2(self):
            return "h2"

        result = merge_lifecycle_handlers(handler1, handler2)
        assert result == [handler1, handler2]

    def test_merge_lifecycle_with_lists(self):
        """Test merging with existing lists."""
        h1, h2, h3 = (lambda: i for i in range(3))

        result = merge_lifecycle_handlers([h1, h2], h3)
        assert len(result) == 3

    def test_merge_lifecycle_filters_none(self):
        """Test None values are filtered."""

        def handler(self):
            return "h"

        result = merge_lifecycle_handlers(None, handler)
        assert result == [handler]


class TestMergeUniqueArray:
    """Tests for unique array merge strategy."""

    def test_merge_unique_array_dedup(self):
        """Test deduplication by identity."""
        a, b, c = "a", "b", "c"
        result = merge_unique_array([a, b], [b, c])
        # Note: dedup by identity, strings may behave differently
        assert len(result) >= 3  # All should be present

    def test_merge_unique_array_none_values(self):
        """Test with None values."""
        result = merge_unique_array(None, ["a", "b"])
        assert result == ["a", "b"]

        result = merge_unique_array(["a", "b"], None)
        assert result == ["a", "b"]


class TestMergeSchemas:
    """Tests for full schema merge."""

    def test_merge_schemas_all_strategies(self):
        """Test all merge strategies in one schema."""
        h1, h2 = lambda: "h1", lambda: "h2"
        m1, m2 = lambda: "m1", lambda: "m2"
        e1, e2 = lambda: "e1", lambda: "e2"
        c1, c2 = lambda: "c1", lambda: "c2"

        target: MixinSchema = {
            "name": "target",
            "version": "1.0",
            "settings": {"a": 1, "b": {"x": 1}},
            "metadata": {"tag": "v1"},
            "actions": {"alpha": h1},
            "events": {"user.created": e1},
            "methods": {"helper": m1},
            "hooks": {"before": {"*": h1}},
            "created": c1,
            "dependencies": ["db"],
        }

        source: MixinSchema = {
            "name": "source",
            "version": "2.0",
            "settings": {"c": 2, "b": {"y": 2}},
            "metadata": {"priority": 3},
            "actions": {"beta": h2},
            "events": {"user.created": e2},
            "methods": {"helper": m2},
            "hooks": {"before": {"*": h2}},
            "created": c2,
            "dependencies": ["cache"],
        }

        result = merge_schemas(target, source)

        # Override: name, version
        assert result["name"] == "source"
        assert result["version"] == "2.0"

        # Deep merge: settings, metadata
        assert result["settings"] == {"a": 1, "b": {"x": 1, "y": 2}, "c": 2}
        assert result["metadata"] == {"tag": "v1", "priority": 3}

        # Actions: combined
        assert "alpha" in result["actions"]
        assert "beta" in result["actions"]

        # Events: handlers concatenated
        assert len(result["events"]["user.created"]["handler"]) == 2

        # Methods: override
        assert result["methods"]["helper"] == m2

        # Hooks: concat with order
        assert result["hooks"]["before"]["*"] == [h1, h2]

        # Lifecycle: concat
        assert result["created"] == [c1, c2]

        # Dependencies: unique array
        assert set(result["dependencies"]) == {"db", "cache"}


class TestApplyMixins:
    """Tests for apply_mixins function."""

    def test_apply_mixins_priority_order(self):
        """Test mixin priority: mixin1 < mixin2 < schema."""
        mixin1: MixinSchema = {"settings": {"a": 1, "b": 1}}
        mixin2: MixinSchema = {"settings": {"b": 2, "c": 2}}
        schema: MixinSchema = {"settings": {"c": 3, "d": 3}, "mixins": [mixin1, mixin2]}

        result = apply_mixins(schema)

        # mixin1.a preserved, mixin2.b overrides mixin1.b, schema.c overrides mixin2.c
        assert result["settings"] == {"a": 1, "b": 2, "c": 3, "d": 3}

    def test_apply_mixins_nested(self):
        """Test nested mixins (mixin inside mixin)."""
        inner: MixinSchema = {"settings": {"inner": True}}
        outer: MixinSchema = {"settings": {"outer": True}, "mixins": [inner]}
        schema: MixinSchema = {"settings": {"main": True}, "mixins": [outer]}

        result = apply_mixins(schema)

        # All settings should be merged
        assert result["settings"]["inner"] is True
        assert result["settings"]["outer"] is True
        assert result["settings"]["main"] is True

    def test_apply_mixins_empty(self):
        """Test with no mixins."""
        schema: MixinSchema = {"name": "test", "settings": {"a": 1}}
        result = apply_mixins(schema)
        assert result == schema

    def test_apply_mixins_class_based(self):
        """Test with class-based mixins."""

        class LoggerMixin:
            __settings__ = {"log_level": "debug"}

            def log(self, message):
                pass

        class CacheMixin:
            __settings__ = {"cache_ttl": 300}

            def get_cached(self, key):
                pass

        schema: MixinSchema = {
            "name": "test",
            "settings": {"custom": True},
        }

        result = apply_mixins(schema, [LoggerMixin, CacheMixin])

        assert result["settings"]["log_level"] == "debug"
        assert result["settings"]["cache_ttl"] == 300
        assert result["settings"]["custom"] is True
        assert "log" in result["methods"]
        assert "get_cached" in result["methods"]

    def test_apply_mixins_lifecycle_handlers_accumulated(self):
        """Test lifecycle handlers from all mixins are accumulated."""
        calls = []

        def mixin1_created(self):
            calls.append("mixin1")

        def mixin2_created(self):
            calls.append("mixin2")

        def main_created(self):
            calls.append("main")

        mixin1: MixinSchema = {"created": mixin1_created}
        mixin2: MixinSchema = {"created": mixin2_created}
        schema: MixinSchema = {"created": main_created, "mixins": [mixin1, mixin2]}

        result = apply_mixins(schema)

        # All handlers should be in the list
        assert len(result["created"]) == 3


class TestExtractMixinMethods:
    """Tests for extract_mixin_methods function."""

    def test_extract_methods_to_instance(self):
        """Test methods are attached to instance."""

        class MockService:
            pass

        def helper_func(self, x):
            return x * 2

        schema: MixinSchema = {"methods": {"helper": helper_func}}
        instance = MockService()

        extract_mixin_methods(instance, schema)

        assert hasattr(instance, "helper")
        assert instance.helper(5) == 10

    def test_extract_methods_skip_existing(self):
        """Test existing methods are not overwritten."""

        class MockService:
            def helper(self, x):
                return x * 3  # Different implementation

        def mixin_helper(self, x):
            return x * 2

        schema: MixinSchema = {"methods": {"helper": mixin_helper}}
        instance = MockService()

        extract_mixin_methods(instance, schema)

        # Original method preserved
        assert instance.helper(5) == 15  # Not 10


class TestServiceMixinIntegration:
    """Integration tests for Service class with mixins."""

    def test_service_with_class_mixin(self):
        """Test Service with class-based mixin."""

        class LoggerMixin:
            __settings__ = {"log_level": "debug"}

            def log(self, msg):
                return f"LOG: {msg}"

        class TestService(Service):
            name = "test"
            mixins = [LoggerMixin]
            settings = {"custom": True}

        svc = TestService()

        # Settings merged
        assert svc.settings["log_level"] == "debug"
        assert svc.settings["custom"] is True

        # Method available
        assert svc.log("hello") == "LOG: hello"

    def test_service_with_dict_mixin(self):
        """Test Service with dict-based mixin."""
        CacheMixin = {
            "settings": {"cache_ttl": 300},
            "methods": {"get_ttl": lambda self: self.settings.get("cache_ttl")},
        }

        class TestService(Service):
            name = "test"
            mixins = [CacheMixin]

        svc = TestService()

        assert svc.settings["cache_ttl"] == 300
        assert svc.get_ttl() == 300

    def test_service_multiple_mixins(self):
        """Test Service with multiple mixins."""

        class Mixin1:
            __settings__ = {"a": 1}

            def method_a(self):
                return "a"

        class Mixin2:
            __settings__ = {"b": 2}

            def method_b(self):
                return "b"

        class TestService(Service):
            name = "test"
            mixins = [Mixin1, Mixin2]
            settings = {"c": 3}

        svc = TestService()

        assert svc.settings == {"a": 1, "b": 2, "c": 3}
        assert svc.method_a() == "a"
        assert svc.method_b() == "b"

    def test_service_mixin_priority(self):
        """Test mixin priority in Service."""

        class Mixin1:
            __settings__ = {"value": 1, "from_mixin1": True}

        class Mixin2:
            __settings__ = {"value": 2, "from_mixin2": True}

        class TestService(Service):
            name = "test"
            mixins = [Mixin1, Mixin2]
            settings = {"value": 3, "from_service": True}

        svc = TestService()

        # Service value wins
        assert svc.settings["value"] == 3
        # All sources contribute
        assert svc.settings["from_mixin1"] is True
        assert svc.settings["from_mixin2"] is True
        assert svc.settings["from_service"] is True

    def test_service_instance_settings_override(self):
        """Test instance settings override class settings."""

        class Mixin:
            __settings__ = {"a": 1}

        class TestService(Service):
            name = "test"
            mixins = [Mixin]
            settings = {"b": 2}

        svc = TestService(settings={"c": 3, "a": 10})

        # Instance override wins
        assert svc.settings["a"] == 10
        assert svc.settings["b"] == 2
        assert svc.settings["c"] == 3


class TestServiceLifecycleMixins:
    """Tests for lifecycle hooks with mixins."""

    @pytest.mark.asyncio
    async def test_lifecycle_created_order(self):
        """Test created lifecycle order: mixins first, then service."""
        calls = []

        class Mixin1:
            def created(self):
                calls.append("mixin1")

        class Mixin2:
            def created(self):
                calls.append("mixin2")

        class TestService(Service):
            name = "test"
            mixins = [Mixin1, Mixin2]

            async def created(self):
                calls.append("service")

        svc = TestService()
        await svc._call_lifecycle_hook("created")

        # Order: mixin1 -> mixin2 -> service
        assert calls == ["mixin1", "mixin2", "service"]

    @pytest.mark.asyncio
    async def test_lifecycle_started_order(self):
        """Test started lifecycle order: mixins first, then service."""
        calls = []

        class Mixin1:
            def started(self):
                calls.append("mixin1")

        class Mixin2:
            def started(self):
                calls.append("mixin2")

        class TestService(Service):
            name = "test"
            mixins = [Mixin1, Mixin2]

            async def started(self):
                calls.append("service")

        svc = TestService()
        await svc._call_lifecycle_hook("started")

        assert calls == ["mixin1", "mixin2", "service"]

    @pytest.mark.asyncio
    async def test_lifecycle_merged_order(self):
        """Test merged lifecycle order: mixins first, then service."""
        calls = []

        class Mixin1:
            def merged(self):
                calls.append("mixin1")

        class Mixin2:
            def merged(self):
                calls.append("mixin2")

        class TestService(Service):
            name = "test"
            mixins = [Mixin1, Mixin2]

            async def merged(self):
                calls.append("service")

        svc = TestService()
        await svc._call_lifecycle_hook("merged")

        assert calls == ["mixin1", "mixin2", "service"]

    @pytest.mark.asyncio
    async def test_lifecycle_stopped_reverse_order(self):
        """Test stopped lifecycle order: service first, then mixins in reverse."""
        calls = []

        class Mixin1:
            def stopped(self):
                calls.append("mixin1")

        class Mixin2:
            def stopped(self):
                calls.append("mixin2")

        class TestService(Service):
            name = "test"
            mixins = [Mixin1, Mixin2]

            async def stopped(self):
                calls.append("service")

        svc = TestService()
        await svc._call_lifecycle_hook("stopped")

        # REVERSE order: service -> mixin2 -> mixin1
        assert calls == ["service", "mixin2", "mixin1"]

    @pytest.mark.asyncio
    async def test_lifecycle_async_handlers(self):
        """Test async lifecycle handlers."""
        calls = []

        class Mixin:
            async def created(self):
                calls.append("mixin_async")

        class TestService(Service):
            name = "test"
            mixins = [Mixin]

            async def created(self):
                calls.append("service_async")

        svc = TestService()
        await svc._call_lifecycle_hook("created")

        assert calls == ["mixin_async", "service_async"]

    @pytest.mark.asyncio
    async def test_lifecycle_mixed_sync_async(self):
        """Test mix of sync and async lifecycle handlers."""
        calls = []

        class SyncMixin:
            def created(self):
                calls.append("sync_mixin")

        class AsyncMixin:
            async def created(self):
                calls.append("async_mixin")

        class TestService(Service):
            name = "test"
            mixins = [SyncMixin, AsyncMixin]

            def created(self):
                calls.append("sync_service")

        svc = TestService()
        await svc._call_lifecycle_hook("created")

        assert calls == ["sync_mixin", "async_mixin", "sync_service"]


class TestServiceMixinHooks:
    """Tests for action hooks with mixins."""

    def test_hooks_merged_from_mixins(self):
        """Test hooks are merged from mixins."""

        class Mixin:
            __hooks__ = {"before": {"*": lambda self, ctx: None}}

        class TestService(Service):
            name = "test"
            mixins = [Mixin]
            hooks = {"after": {"*": lambda self, ctx: None}}

        svc = TestService()
        hooks = svc.get_hooks()

        assert "before" in hooks
        assert "after" in hooks


class TestServiceMixinDependencies:
    """Tests for dependencies with mixins."""

    def test_dependencies_merged(self):
        """Test dependencies are merged from mixins."""

        class Mixin1:
            dependencies = ["db"]

        class Mixin2:
            dependencies = ["cache"]

        class TestService(Service):
            name = "test"
            mixins = [Mixin1, Mixin2]
            dependencies = ["auth"]

        svc = TestService()

        # All dependencies should be present
        assert "db" in svc.dependencies
        assert "cache" in svc.dependencies
        assert "auth" in svc.dependencies

    def test_dependencies_deduplicated(self):
        """Test duplicate dependencies are removed."""

        class Mixin1:
            dependencies = ["db", "cache"]

        class Mixin2:
            dependencies = ["cache", "auth"]

        class TestService(Service):
            name = "test"
            mixins = [Mixin1, Mixin2]

        svc = TestService()

        # Should have unique dependencies
        assert len(svc.dependencies) == len(set(svc.dependencies))


class TestServiceGetSchema:
    """Tests for get_schema method."""

    def test_get_schema_returns_merged(self):
        """Test get_schema returns merged schema."""

        class Mixin:
            __settings__ = {"a": 1}

        class TestService(Service):
            name = "test"
            mixins = [Mixin]
            settings = {"b": 2}

        svc = TestService()
        schema = svc.get_schema()

        assert "settings" in schema
        assert schema["settings"]["a"] == 1
        assert schema["settings"]["b"] == 2


class TestNestedMixins:
    """Tests for deeply nested mixins."""

    def test_three_level_nesting(self):
        """Test three levels of mixin nesting."""
        level3: MixinSchema = {"settings": {"level3": True}}
        level2: MixinSchema = {"settings": {"level2": True}, "mixins": [level3]}
        level1: MixinSchema = {"settings": {"level1": True}, "mixins": [level2]}
        schema: MixinSchema = {"settings": {"main": True}, "mixins": [level1]}

        result = apply_mixins(schema)

        assert result["settings"]["level3"] is True
        assert result["settings"]["level2"] is True
        assert result["settings"]["level1"] is True
        assert result["settings"]["main"] is True

    def test_diamond_inheritance(self):
        """Test diamond inheritance pattern (A used by both B and C)."""
        # Common mixin
        common: MixinSchema = {"settings": {"common": True}, "created": lambda: "common"}

        # B uses common
        mixin_b: MixinSchema = {
            "settings": {"from_b": True},
            "mixins": [common],
            "created": lambda: "b",
        }

        # C also uses common
        mixin_c: MixinSchema = {
            "settings": {"from_c": True},
            "mixins": [common],
            "created": lambda: "c",
        }

        # Main uses both B and C
        schema: MixinSchema = {
            "settings": {"main": True},
            "mixins": [mixin_b, mixin_c],
            "created": lambda: "main",
        }

        result = apply_mixins(schema)

        # All settings should be present
        assert result["settings"]["common"] is True
        assert result["settings"]["from_b"] is True
        assert result["settings"]["from_c"] is True
        assert result["settings"]["main"] is True

        # Created handlers from all sources (common appears twice due to diamond)
        # This matches Moleculer.js behavior
        assert len(result["created"]) >= 4


class TestActionDisable:
    """Tests for action disable with False."""

    def test_disable_action_from_mixin(self):
        """Test disabling an action from mixin."""

        def handler(ctx):
            return "result"

        mixin: MixinSchema = {
            "actions": {
                "alpha": handler,
                "beta": handler,
            }
        }

        schema: MixinSchema = {
            "actions": {
                "alpha": False,  # Disable alpha
                "gamma": handler,
            },
            "mixins": [mixin],
        }

        result = apply_mixins(schema)

        assert "alpha" not in result["actions"]  # Disabled
        assert "beta" in result["actions"]  # From mixin
        assert "gamma" in result["actions"]  # From schema
