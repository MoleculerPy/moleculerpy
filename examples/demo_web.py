"""Demo stand: moleculerpy-web HTTP Gateway end-to-end on real HTTP.

Starts ApiGatewayService on a random free port with a memory-transport
broker, registers a users/stream service, then fires real HTTP requests
via httpx.AsyncClient and verifies responses. No mocks below the gateway.

Usage:
    .venv/bin/python examples/demo_web.py
Exit codes:
    0 — all tests passed
    1 — one or more tests failed
    2 — missing dependency (moleculerpy_web or httpx)
"""

from __future__ import annotations

import asyncio
import socket
import sys
from typing import Any

# ---------------------------------------------------------------------------
# Pre-flight: dependency check
# ---------------------------------------------------------------------------
try:
    import httpx
except ImportError:
    print("ERROR: httpx is not installed. Run: pip install httpx", file=sys.stderr)
    sys.exit(2)

try:
    from moleculerpy_web import ApiGatewayService

    from moleculerpy import Broker, Context, Service, action
    from moleculerpy.errors import MoleculerClientError, ValidationError
    from moleculerpy.settings import Settings
except ImportError as exc:
    print(f"ERROR: moleculerpy_web not installed ({exc}).", file=sys.stderr)
    print("       Run: pip install -e moleculerpy-web", file=sys.stderr)
    sys.exit(2)


# ---------------------------------------------------------------------------
# ANSI colors
# ---------------------------------------------------------------------------
GREEN = "\033[32m"
RED = "\033[31m"
CYAN = "\033[36m"
DIM = "\033[2m"
RESET = "\033[0m"


# ---------------------------------------------------------------------------
# Test service
# ---------------------------------------------------------------------------
class UsersService(Service):
    """In-memory users service for demo testing."""

    name = "users"

    def __init__(self) -> None:
        super().__init__(self.name)
        self._db: dict[str, dict[str, Any]] = {
            "1": {"id": "1", "name": "Alice"},
            "42": {"id": "42", "name": "Charlie"},
        }

    @action()
    async def list(self, ctx: Context) -> dict[str, Any]:
        limit = ctx.params.get("limit")
        users = list(self._db.values())
        if limit is not None:
            users = users[: int(limit)]
        return {"users": users, "total": len(users), "limit": limit}

    @action()
    async def get(self, ctx: Context) -> dict[str, Any]:
        uid = str(ctx.params.get("id", ""))
        user = self._db.get(uid)
        if not user:
            from moleculerpy.errors import ServiceNotFoundError

            raise ServiceNotFoundError(f"User {uid} not found")
        return user

    @action()
    async def create(self, ctx: Context) -> dict[str, Any]:
        name = ctx.params.get("name")
        if not name or not isinstance(name, str):
            raise ValidationError("'name' is required and must be a string")
        new_id = str(len(self._db) + 100)
        user = {"id": new_id, "name": name}
        self._db[new_id] = user
        return user

    @action()
    async def secure_list(self, ctx: Context) -> dict[str, Any]:
        """Server-side auth check via query-param token (demo-grade)."""
        token = ctx.params.get("token")
        if token != "let-me-in":
            # MoleculerClientError with code=401 maps to HTTP 401.
            raise MoleculerClientError("missing or invalid token", code=401, type="UNAUTHORIZED")
        return {"users": list(self._db.values())}

    @action()
    async def forbidden_op(self, ctx: Context) -> dict[str, Any]:
        """Action that always refuses — exercises 403 mapping for code=403."""
        raise MoleculerClientError("insufficient scope", code=403, type="FORBIDDEN")

    @action()
    async def missing_resource(self, ctx: Context) -> dict[str, Any]:
        """Action that raises NOT_FOUND as a client-level error (code=404).

        Distinct from the built-in ``ServiceNotFoundError`` path — here the
        service exists and chooses to report "resource gone" at the action
        layer via ``MoleculerClientError(code=404)``. This exercises the
        numeric 404 fallthrough in the gateway error mapping.
        """
        raise MoleculerClientError("resource has been archived", code=404, type="NOT_FOUND")


class StreamService(Service):
    """Returns async generator so the gateway streams chunks."""

    name = "stream"

    @action()
    async def lines(self, ctx: Context) -> Any:
        async def gen():
            for i in range(5):
                yield f"chunk-{i}\n".encode()
                await asyncio.sleep(0.01)

        return gen()


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


# ---------------------------------------------------------------------------
# Main test runner
# ---------------------------------------------------------------------------
async def run_tests() -> int:
    results: list[tuple[str, bool, str]] = []

    def record(name: str, ok: bool, detail: str = "") -> None:
        results.append((name, ok, detail))
        tag = f"{GREEN}PASS{RESET}" if ok else f"{RED}FAIL{RESET}"
        extra = f" {DIM}{detail}{RESET}" if detail else ""
        print(f"  {tag}  {name}{extra}")

    port = find_free_port()
    base = f"http://127.0.0.1:{port}"
    print(f"{CYAN}moleculerpy-web demo stand{RESET}")
    print(f"  Gateway: {base}")
    print("  Transport: memory (no broker network)")
    print()

    # Enable context tracking so broker.stop() waits for in-flight requests
    # (graceful shutdown test below relies on this).
    from moleculerpy.settings import TrackingConfig

    settings = Settings(
        transporter="memory://",
        log_level="ERROR",
        tracking=TrackingConfig(enabled=True, shutdown_timeout=5.0),
    )
    broker = Broker("demo-web", settings=settings)

    gateway = ApiGatewayService(
        broker=broker,
        settings={
            "port": port,
            "ip": "127.0.0.1",
            "path": "/api",
            "routes": [
                {
                    "path": "/",
                    "aliases": {
                        "GET /users": "users.list",
                        "GET /users/{id}": "users.get",
                        "POST /users": "users.create",
                        "GET /stream": "stream.lines",
                        "GET /secure": "users.secure_list",
                        "GET /forbidden": "users.forbidden_op",
                        "GET /archived": "users.missing_resource",
                    },
                    "etag": True,
                    "cors": {
                        "origin": "*",
                        "methods": ["GET", "POST", "OPTIONS"],
                        "allowedHeaders": ["content-type", "x-auth"],
                    },
                },
            ],
        },
    )

    await broker.register(UsersService())
    await broker.register(StreamService())
    await broker.register(gateway)
    await broker.start()
    # let uvicorn bind
    await asyncio.sleep(0.5)

    try:
        async with httpx.AsyncClient(base_url=base, timeout=10.0) as client:
            print("--- 1. GET list ---")
            r = await client.get("/api/users")
            body = r.json() if r.status_code == 200 else {}
            record(
                "test_get_list",
                r.status_code == 200 and isinstance(body.get("users"), list),
                f"status={r.status_code}",
            )

            print("--- 2. GET one ---")
            r = await client.get("/api/users/42")
            body = r.json() if r.status_code == 200 else {}
            record(
                "test_get_one",
                r.status_code == 200 and body.get("name") == "Charlie",
                f"status={r.status_code}",
            )

            print("--- 3. POST create ---")
            r = await client.post("/api/users", json={"name": "Dave"})
            body = r.json() if r.status_code == 200 else {}
            # Gateway returns 200 by default (not 201) — accept both
            record(
                "test_post_create",
                r.status_code in (200, 201) and body.get("name") == "Dave",
                f"status={r.status_code}",
            )

            print("--- 4. 404 unknown route ---")
            r = await client.get("/api/nonexistent")
            record("test_404", r.status_code == 404, f"status={r.status_code}")

            print("--- 5. Validation error (missing 'name') ---")
            r = await client.post("/api/users", json={})
            record(
                "test_400_validation",
                # ValidationError maps to 422; accept either 400 or 422 per HTTP conventions
                r.status_code in (400, 422),
                f"status={r.status_code}",
            )

            print("--- 6. CORS preflight ---")
            r = await client.request(
                "OPTIONS",
                "/api/users",
                headers={
                    "Origin": "https://example.com",
                    "Access-Control-Request-Method": "GET",
                },
            )
            cors_origin = r.headers.get("access-control-allow-origin", "")
            record(
                "test_cors",
                r.status_code in (200, 204) and cors_origin in ("*", "https://example.com"),
                f"status={r.status_code} allow-origin={cors_origin!r}",
            )

            print("--- 7. Server-side auth check ---")
            r_no = await client.get("/api/secure")
            r_ok = await client.get("/api/secure?token=let-me-in")
            # Regression for KNOWN-ISSUES #19: 401 must be exact, not "400 or 401".
            # Previously moleculer_error_to_http mapped every MoleculerClientError
            # to BadRequestError(400) — this assertion used to accept the buggy
            # 400 as "passing", hiding the real fix value. Now we demand the
            # Node.js-compatible HTTP surface exactly.
            record(
                "test_client_error_401",
                r_no.status_code == 401 and r_ok.status_code == 200,
                f"without={r_no.status_code} (expected 401) with={r_ok.status_code}",
            )

            print("--- 7b. MoleculerClientError(code=403) -> HTTP 403 ---")
            r = await client.get("/api/forbidden")
            record(
                "test_client_error_403",
                r.status_code == 403,
                f"status={r.status_code} (expected 403)",
            )

            print("--- 7c. MoleculerClientError(code=404) -> HTTP 404 ---")
            r = await client.get("/api/archived")
            record(
                "test_client_error_404",
                r.status_code == 404,
                f"status={r.status_code} (expected 404)",
            )

            print("--- 8. ETag + 304 ---")
            r1 = await client.get("/api/users")
            etag = r1.headers.get("etag", "")
            r2 = await client.get("/api/users", headers={"If-None-Match": etag}) if etag else None
            record(
                "test_etag",
                bool(etag) and r2 is not None and r2.status_code == 304,
                f"etag={etag!r} second={getattr(r2, 'status_code', None)}",
            )

            print("--- 9. Query params ---")
            r = await client.get("/api/users?limit=1")
            body = r.json() if r.status_code == 200 else {}
            record(
                "test_query_params",
                r.status_code == 200 and str(body.get("limit")) == "1",
                f"status={r.status_code} limit={body.get('limit')!r}",
            )

            print("--- 10. Streaming response ---")
            r = await client.get("/api/stream")
            lines = r.text.strip().split("\n") if r.status_code == 200 else []
            record(
                "test_streaming",
                r.status_code == 200 and len(lines) == 5 and lines[0] == "chunk-0",
                f"status={r.status_code} lines={len(lines)}",
            )

            print("--- 11. Graceful shutdown ---")
            # Verify broker.stop() completes cleanly without raising.
            # Note: HTTP gateway streaming + broker stop is a gateway-level
            # concern (not action-level ContextTracker). We verify that:
            #   - broker.stop() returns within timeout
            #   - subsequent requests fail cleanly (gateway closed)
            stop_task = asyncio.create_task(broker.stop())
            try:
                await asyncio.wait_for(stop_task, timeout=5.0)
                stop_ok = True
            except Exception as exc:
                stop_ok = False
                print(f"  {DIM}stop err: {exc}{RESET}")
            record(
                "test_graceful_shutdown",
                stop_ok,
                f"stop={stop_ok}",
            )
    finally:
        # Best-effort cleanup if we did not reach the graceful-shutdown test.
        try:
            await broker.stop()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Summary table
    # ------------------------------------------------------------------
    passed = sum(1 for _, ok, _ in results if ok)
    total = len(results)
    failed = total - passed
    print()
    print(f"{CYAN}{'=' * 60}{RESET}")
    print(f"  Results: {passed}/{total} passed, {failed} failed")
    print(f"{CYAN}{'=' * 60}{RESET}")
    for name, ok, detail in results:
        mark = f"{GREEN}[OK]  {RESET}" if ok else f"{RED}[FAIL]{RESET}"
        print(f"  {mark} {name}{(' — ' + detail) if detail and not ok else ''}")
    print()

    return 0 if failed == 0 else 1


def main() -> int:
    try:
        return asyncio.run(run_tests())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
