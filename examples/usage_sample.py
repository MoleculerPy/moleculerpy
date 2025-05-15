import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # import moleculerpy
from moleculerpy.broker import Broker
from moleculerpy.context import Context
from moleculerpy.decorators import action, event
from moleculerpy.service import Service


class MyService(Service):
    name = "myService"

    def __init__(self):
        super().__init__(self.name)

    # TODO: add validation
    @action(params=[])
    async def foo(self, ctx: Context):
        return 100

    @action(params=[])
    async def bar(self, ctx: Context):
        a = await ctx.call("myService.foo", {})  # internal context call
        res = await ctx.call("math.add", {"a": a, "b": 50})  # remote service call
        return res

    @event()
    async def checked(self, ctx: Context):
        print("checked called")

    @event(name="foo.bar.lol")
    async def complex_handler(self, ctx: Context):
        print("complex handler called")


# Example usage
import asyncio


async def main():
    broker = Broker("broker1")

    mysvc = MyService()

    await broker.register(mysvc)

    await broker.start()

    res = await broker.call("myService.foo", {"param1": "aaaa"})

    broker.logger.info(f"local res is {res}")

    broker.logger.info("waiting for services")

    await broker.wait_for_services(["math"])

    broker.logger.info("remote services ready")

    mathRes = await broker.call(
        "math.add", {"a": 1, "b": 50}
    )  # remote action on tests/intetgration

    broker.logger.info(f"math res is {mathRes}")

    barREs = await broker.call("myService.bar", {}, meta={"tenant": "foobar-company"})

    broker.logger.info(f"bar res is {barREs}")

    await broker.emit("checked", {})

    await broker.wait_for_shutdown()


asyncio.run(main())
