"""
Implements a Key Expressions Language similar to zenoh:
https://github.com/eclipse-zenoh/roadmap/blob/main/rfcs/ALL/Key%20Expressions.md
"""

from dataclasses import dataclass
from typing import Any, Dict
import re
import asyncio
import logging
import rembus.core as rc
import rembus.protocol as rp
from rembus.router import Router, top_router

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SpaceTwin:
    """Map a space pattern to a twin."""

    space: str
    twid: str


def build_space_re(topic: str) -> re.Pattern:
    """Create a regex from a topic pattern with wildcards.

    Supports:
    - * → exactly one non-empty segment
    - ** → any (possibly empty) character sequence, including `/`
    Wildcards are valid only as a whole `/`-separated segment.
    """
    parts = []
    for seg in topic.split("/"):
        if seg == "**":
            parts.append(".*")
        elif seg == "*":
            parts.append("[^/]+")
        else:
            parts.append(re.escape(seg))
    return re.compile("^" + "/".join(parts) + "$", re.DOTALL)


GLOB_STAR = build_space_re("**")


class KeySpaceRouter(rc.Supervised):
    """The router for keyspaces."""

    spaces: Dict[re.Pattern[str], set[SpaceTwin]]

    broker: Router

    def __init__(self):
        super().__init__()
        self.broker: Router
        self.has_glob_star = False
        self.spaces = {}
        self.inbox: asyncio.Queue[Any] = asyncio.Queue()

    def __str__(self):
        return f"keyspace::{top_router(self)}"

    async def setup_twin(self, twin):
        """Setup the twin subscriptions to keyspaces."""
        router: Router = self.broker
        for topic in router.subscribers:
            if twin in router.subscribers[topic]:
                logger.debug(
                    "[ksrouter] subscribing twin %s to topic%s", twin, topic
                )
                await self.subscribe_handler(twin, topic)

    async def subscribe_handler(self, component, topic):
        """Setup the keyspace regular expression from the message topic"""
        logger.debug("[keyspace] registering %s", topic)

        if "*" in topic and rp.valid_pattern(topic):
            retopic = build_space_re(topic)
            if retopic == GLOB_STAR:
                self.has_glob_star = True

            logger.debug("[keyspace] compiled regex %s", retopic)
            if retopic not in self.spaces:
                self.spaces[retopic] = set()

            self.spaces[retopic].add(SpaceTwin(topic, component.twkey))

    async def unsubscribe_handler(self, component, topic):
        """Remove the keyspace subscription for message topic"""
        logger.debug("[keyspace] unregistering %s", topic)

        if "*" in topic:
            retopic = build_space_re(topic)

            if retopic in self.spaces:
                el = SpaceTwin(topic, component.twkey)
                if el in self.spaces[retopic]:
                    self.spaces[retopic].remove(el)

    async def broadcast(self, topic, msg, space_twins):
        """Brodcast the message to all subscribed spaces."""
        for space_twin in space_twins:
            pattern = space_twin.space
            # The first argument is the originating topic
            datas = [topic, *msg.data]
            twid = space_twin.twid
            logger.debug(
                "[keyspace] publish to %s topic and data:%s %s",
                twid,
                pattern,
                datas,
            )

            # Evaluate local subscribers
            if pattern in self.broker.handler:
                if twid in self.broker.id_twin:
                    tw = self.broker.id_twin[twid]
                    if tw.isbroker():
                        await self.broker.evaluate(tw, pattern, datas)

            if twid in self.broker.twins:
                tw = self.broker.id_twin[twid]
                if tw.isopen():
                    if tw.ismqtt:
                        if not msg.from_mqtt:
                            await tw.publish(topic, *msg.data)
                    else:
                        await tw.publish(pattern, *datas)

    async def publish_interceptor(self, msg):
        """Parse the topic and dispatch to all twins subscribed to spaces
        with regex matching the topic"""
        topic = msg.topic
        for space_regex, space_twins in self.spaces.items():
            if space_regex.match(topic) is not None:
                await self.broadcast(topic, msg, space_twins)

    async def _task_impl(self) -> None:
        """Override in subclasses for supervised task impl."""
        logger.debug("[keyspace] started")
        self.broker = top_router(self)
        while True:
            msg = await self.inbox.get()
            logger.debug("[%s] recv: %s", self, msg)
            if msg == "shutdown":
                logger.debug("[%s] keyspace router shutting down", self)
                break
            if isinstance(msg, rp.AdminMsg) and rp.COMMAND in msg.data:
                if msg.data[rp.COMMAND] == rp.SUBSCRIBE_CMD:
                    await self.subscribe_handler(msg.twin, msg.topic)
                elif msg.data[rp.COMMAND] == rp.UNSUBSCRIBE_CMD:
                    await self.unsubscribe_handler(msg.twin, msg.topic)
            elif isinstance(msg, rp.PubSubMsg):
                await self.publish_interceptor(msg)

            # route to downstream broker
            await self.downstream.inbox.put(msg)  # type: ignore
