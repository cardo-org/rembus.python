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

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SpaceTwin:
    """Map a space pattern to a twin."""

    space: str
    twid: str


def build_space_re(topic):
    """Create a regex from a topic pattern."""
    s = topic
    s = s.replace("/**/", "(.*)")
    s = s.replace("**", "(.*)")
    s = s.replace("*", "([^/]+)")
    return re.compile(f"^{s}$")


class KeySpaceRouter(rc.Supervised):
    """The router for keyspaces."""

    spaces: Dict[re.Pattern[str], set[SpaceTwin]]

    broker: rc.Router

    def __init__(self):
        super().__init__()
        self.spaces = {}
        self.inbox: asyncio.Queue[Any] = asyncio.Queue()

    def __str__(self):
        return f"keyspace::{rc.top_router(self)}"

    async def subscribe_handler(self, msg):
        """Setup the keyspace regular expression from the message topic"""
        component = msg.twin
        topic = msg.topic
        logger.info("[keyspace] registering %s", topic)

        if "*" in topic:
            retopic = build_space_re(topic)

            if retopic not in self.spaces:
                self.spaces[retopic] = set()

            self.spaces[retopic].add(SpaceTwin(topic, component.twkey))

    async def unsubscribe_handler(self, msg):
        """Remove the keyspace subscription for message topic"""
        component = msg.twin
        topic = msg.topic
        logger.info("[keyspace] unregistering %s", topic)

        if "*" in topic:
            retopic = build_space_re(topic)

            if retopic in self.spaces:
                el = SpaceTwin(topic, component.twkey)
                if el in self.spaces[retopic]:
                    self.spaces[retopic].remove(el)

    async def publish_interceptor(self, msg):
        """Parse the topic and dispatch to all twins subscribed to spaces
        with regex matching the topic"""
        topic = msg.topic
        if "/" in topic:
            for space_regex, space_twins in self.spaces.items():
                m = space_regex.match(topic)
                if m is not None:
                    unsealed = True

                    # m.groups() == captures in Julia
                    for capture in m.groups():
                        if "@" in capture:
                            # a regex chunk matched a verbatim chunk → reject
                            unsealed = False
                            break

                    if unsealed:
                        to_remove = []
                        # for pattern, twid in space_twins:
                        for space_twin in space_twins:
                            pattern = space_twin.space
                            logger.debug(
                                "[keyspace] publish to %s: %s, %s",
                                pattern,
                                topic,
                                msg.data,
                            )
                            twid = space_twin.twid
                            if twid in self.broker.id_twin:
                                tw = self.broker.id_twin[twid]
                                if tw.isopen():
                                    await tw.publish(pattern, topic, *msg.data)
                            else:
                                # cleanup, the twin has gone
                                to_remove.append(space_twin)

                        for el in to_remove:
                            space_twins.remove(el)

    async def _task_impl(self) -> None:
        """Override in subclasses for supervised task impl."""
        logger.debug("[keyspace] started")
        self.broker = rc.top_router(self)
        while True:
            msg = await self.inbox.get()
            logger.debug("[%s] recv: %s", self, msg)
            if isinstance(msg, rp.AdminMsg) and rp.COMMAND in msg.data:
                if msg.data[rp.COMMAND] == rp.ADD_INTEREST:
                    await self.subscribe_handler(msg)
                elif msg.data[rp.COMMAND] == rp.REMOVE_INTEREST:
                    await self.unsubscribe_handler(msg)
            elif isinstance(msg, rp.PubSubMsg):
                await self.publish_interceptor(msg)

            # route to downstream broker
            await self.downstream.inbox.put(msg)  # type: ignore
