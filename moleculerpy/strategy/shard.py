"""Shard load balancing strategy for MoleculerPy.

This module implements consistent hashing strategy that routes requests
with the same shard key to the same node, following Moleculer.js patterns.
"""

from __future__ import annotations

import hashlib
import random
from functools import lru_cache
from typing import TYPE_CHECKING

from .base import BaseStrategy

if TYPE_CHECKING:
    from collections.abc import Sequence

    from ..context import Context
    from ..registry import Action


class ShardStrategy(BaseStrategy):
    """Load balancing strategy using consistent hashing.

    This strategy ensures requests with the same shard key are always
    routed to the same node, enabling stateful services and caching.

    Compatible with Moleculer.js ShardStrategy.

    Options:
        shardKey (str | None): Key to extract from params or meta (default: None)
            - Plain key: extracted from ctx.params (e.g., "userId")
            - Key starting with "#": extracted from ctx.meta (e.g., "#tenantId")
            - None: falls back to random selection (Moleculer.js compatible)
        vnodes (int): Number of virtual nodes per real node (default: 10)
        ringSize (int): LRU cache size for key->node mappings (default: 1000)

    Example:
        >>> strategy = ShardStrategy({"shardKey": "userId", "vnodes": 20})
        >>> endpoint = strategy.select(endpoints, ctx)
    """

    __slots__ = (
        "_get_node_for_key",
        "_node_ids_cache",
        "_node_set",
        "_ring",
        "_shard_key",
        "_vnodes",
    )

    @staticmethod
    def _as_int(value: object, default: int) -> int:
        if isinstance(value, (int, float, str, bytes, bytearray)):
            try:
                return int(value)
            except (TypeError, ValueError):
                return default
        return default

    def __init__(self, opts: dict[str, object] | None = None) -> None:
        """Initialize ShardStrategy.

        Args:
            opts: Strategy options (shardKey, vnodes, ringSize)

        Caching Strategy:
            The strategy employs multi-level caching for optimal performance:

            1. **Ring Cache** (`_ring`, `_node_set`):
               - Hash ring is rebuilt only when node topology changes
               - `_node_set` (frozenset) enables O(1) equality comparison
               - Ring rebuild is O(n * vnodes) but happens rarely

            2. **Key→Node LRU Cache** (`_get_node_for_key`):
               - Maps shard key values to target node IDs
               - Configurable size via `ringSize` option (default: 1000)
               - Cache is cleared when ring is rebuilt

            3. **Node IDs Cache** (`_node_ids_cache`):
               - Caches the ordered list of node IDs from endpoints
               - Avoids repeated dict.fromkeys() on every select() call
               - Invalidated when topology changes

            Performance Characteristics:
            - Stable topology: O(1) for cached keys, O(log n) for new keys
            - Topology change: O(n * vnodes) one-time rebuild cost
            - Memory: O(n * vnodes) for ring + O(ringSize) for key cache
        """
        super().__init__(opts)
        # Moleculer.js: shardKey defaults to null (requires explicit configuration)
        shard_key = opts.get("shardKey") if opts else None
        self._shard_key: str | None = str(shard_key) if shard_key is not None else None
        self._vnodes = self._as_int(opts.get("vnodes", 10), 10) if opts else 10
        ring_size = self._as_int(opts.get("ringSize", 1000), 1000) if opts else 1000

        # Ring will be built lazily when endpoints change
        self._ring: list[tuple[int, str]] = []  # (hash_value, node_id)
        self._node_set: frozenset[str] = frozenset()

        # Cache for node_ids list (avoids repeated dict.fromkeys on every call)
        self._node_ids_cache: list[str] = []

        # LRU cache for key->node_id mappings
        self._get_node_for_key = lru_cache(maxsize=ring_size)(self._lookup_node)

    def _hash(self, key: str) -> int:
        """Compute hash value for a key.

        Uses MD5 truncated to 32 bits for consistent hashing.

        Args:
            key: String to hash

        Returns:
            32-bit integer hash value
        """
        return int(hashlib.md5(key.encode()).hexdigest()[:8], 16)

    def _build_ring(self, node_ids: Sequence[str]) -> None:
        """Build consistent hash ring with virtual nodes.

        This method is called when node topology changes. It rebuilds:
        - The hash ring with virtual nodes
        - The node_set for fast comparison
        - Clears the key→node LRU cache

        Args:
            node_ids: List of unique node IDs to include in ring

        Complexity:
            Time: O(n * vnodes * log(n * vnodes)) due to sorting
            Space: O(n * vnodes) for ring storage
        """
        ring: list[tuple[int, str]] = []

        for node_id in node_ids:
            for i in range(self._vnodes):
                # Create virtual node key: "{node_id}#{vnode_index}"
                vnode_key = f"{node_id}#{i}"
                hash_value = self._hash(vnode_key)
                ring.append((hash_value, node_id))

        # Sort ring by hash value for binary search
        ring.sort(key=lambda x: x[0])
        self._ring = ring
        self._node_set = frozenset(node_ids)
        self._node_ids_cache = list(node_ids)  # Cache ordered node IDs

        # Clear LRU cache since ring changed
        self._get_node_for_key.cache_clear()

    def _lookup_node(self, key: str) -> str | None:
        """Find node for a key using consistent hashing.

        Uses binary search on the sorted ring.

        Args:
            key: Shard key value

        Returns:
            Node ID or None if ring is empty
        """
        if not self._ring:
            return None

        key_hash = self._hash(key)

        # Binary search for first hash >= key_hash
        left, right = 0, len(self._ring)
        while left < right:
            mid = (left + right) // 2
            if self._ring[mid][0] < key_hash:
                left = mid + 1
            else:
                right = mid

        # Wrap around if we went past the end
        if left >= len(self._ring):
            left = 0

        return self._ring[left][1]

    def _get_shard_key_value(self, ctx: Context | None) -> str | None:
        """Extract shard key value from context.

        Args:
            ctx: Request context

        Returns:
            Shard key value as string, or None if not found/not configured
        """
        # Moleculer.js: if shardKey is null, return null (fallback to random)
        if self._shard_key is None:
            return None

        if ctx is None:
            return None

        if self._shard_key.startswith("#"):
            # Extract from meta (e.g., "#tenantId" -> ctx.meta["tenantId"])
            meta_key = self._shard_key[1:]
            value = ctx.meta.get(meta_key)
        else:
            # Extract from params
            value = ctx.params.get(self._shard_key)

        return str(value) if value is not None else None

    def select(
        self,
        endpoints: Sequence[Action],
        ctx: Context | None = None,
    ) -> Action | None:
        """Select endpoint using consistent hashing.

        Same shard key always routes to the same node (if available).

        Algorithm:
            1. Extract unique node IDs from endpoints (preserving order)
            2. Check if topology changed via frozenset comparison (O(1))
            3. If changed, rebuild ring and update caches
            4. Extract shard key from context (params or meta)
            5. Lookup target node via cached ring (O(log n) or O(1) if cached)
            6. Find and return endpoint for target node

        Args:
            endpoints: List of available action endpoints
            ctx: Request context containing shard key

        Returns:
            Selected endpoint, or None if list is empty

        Note:
            Node ID extraction (dict.fromkeys) runs on every call to detect
            topology changes. This is O(n) but necessary for correctness.
            If topology is stable, ring lookup is O(1) via LRU cache.
        """
        if not endpoints:
            return None

        if len(endpoints) == 1:
            return endpoints[0]

        # Convert to list once for reuse
        endpoints_list = list(endpoints)

        # Get unique node IDs from endpoints (preserves insertion order)
        # Note: This runs every call to detect topology changes
        node_ids = list(dict.fromkeys(ep.node_id for ep in endpoints_list))

        # Rebuild ring only if nodes changed (frozenset comparison is O(n) worst case)
        current_set = frozenset(node_ids)
        if current_set != self._node_set:
            self._build_ring(node_ids)

        # Get shard key value
        key_value = self._get_shard_key_value(ctx)

        if key_value is None:
            # No shard key configured or not found - fallback to random
            return endpoints_list[random.randint(0, len(endpoints_list) - 1)]

        # Find target node (O(1) if cached, O(log n) otherwise)
        target_node_id = self._get_node_for_key(key_value)

        if target_node_id is None:
            return endpoints_list[random.randint(0, len(endpoints_list) - 1)]

        # Find endpoint for target node
        for ep in endpoints_list:
            if ep.node_id == target_node_id:
                return ep

        # Target node not found (shouldn't happen if ring is correct)
        return endpoints_list[random.randint(0, len(endpoints_list) - 1)]

    def __repr__(self) -> str:
        shard_key_str = f"'{self._shard_key}'" if self._shard_key else "None"
        return (
            f"ShardStrategy(shardKey={shard_key_str}, "
            f"vnodes={self._vnodes}, "
            f"nodes={len(self._node_set)})"
        )
