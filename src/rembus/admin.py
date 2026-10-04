import logging
import rembus.protocol as rp

logger = logging.getLogger(__name__)


def upsert_twin(lst, twin):
    """Insert a new twin or update the list with the reconnected twin."""
    if twin in lst:
        idx = lst.index(twin)
        lst.pop(idx)
        lst.insert(idx, twin)
    else:
        lst.append(twin)


def add_exposer(router, twin, topic):
    logger.debug("[%s] adding [%s] exposer for topic [%s]", router, twin, topic)
    if topic not in router.exposers:
        router.exposers[topic] = []
    upsert_twin(router.exposers[topic], twin)


def remove_exposer(router, twin, topic):
    if topic in router.exposers:
        if twin in router.exposers[topic]:
            router.exposers[topic].remove(twin)
    logger.debug(
        "[%s] removed [%s] exposer for topic [%s]", router, twin, topic
    )


def add_subscriber(router, twin, topic, msgfrom):
    logger.debug(
        "[%s] adding [%s] subscriber for topic [%s]", router, twin, topic
    )
    if topic not in router.subscribers:
        router.subscribers[topic] = []

    twin.msg_from[topic] = float(msgfrom)
    upsert_twin(router.subscribers[topic], twin)


def remove_subscriber(router, twin, topic):
    if topic in router.subscribers:
        if twin in router.subscribers[topic]:
            router.subscribers[topic].remove(twin)
    logger.debug(
        "[%s] removed [%s] subscriber for topic [%s]", router, twin, topic
    )


async def color_admin(tw, msg: rp.AdminMsg):
    """Send `msg` to the neighbor `tw`, after "coloring" it: append
    `tw.rid` to `msg.data["touch"]` (creating the list if absent) just
    before transmitting.

    This marks a link as already crossed by an admin command, so that
    `tw` (or whichever router/component re-broadcasts the message
    further) never bounces it back to a twin whose id is already in
    `touch`. See `admin_broadcast` and the Mesh Routing and Forwarding
    documentation for the full flood/anti-loop scheme.
    """
    logger.debug("[%s] coloring admin message %s", tw, msg.data)
    msg.data.setdefault("touch", []).append(tw.rid)
    await tw.send(msg)


async def admin_broadcast(router, twin, msg: rp.AdminMsg):
    """Flood `msg` to every named, open, directly-connected neighbor of
    `router` (`router.id_twin`), except:

    - `twin` itself, the originator of the command;
    - any neighbor whose `rid` is already present in
      `msg.data["touch"]` (already reached through another path, see
      `color_admin`).

    Each twin that is actually sent the message is marked via
    `color_admin` before transmission. This is the link-level half of
    the mesh flood; the router-level half (stopping re-processing once
    a router has already seen the command) is `mark_and_broadcast`,
    which calls this function.
    """
    logger.debug("[%s] admin command broadcast: %s", twin, msg)
    touched = msg.data.get("touch", [])
    for tw in list(router.id_twin.values()):
        if tw.uid.hasname and tw.rid != twin.rid and tw.rid not in touched:
            if not tw.isopen():
                logger.debug("[%s] not sent: socket is closed", tw)
                continue
            logger.debug("[%s] broadcasting %s to %s", twin, msg, tw)
            await color_admin(tw, msg)


async def mark_and_broadcast(router, twin, msg: rp.AdminMsg) -> bool:
    """Router-level loop guard around `admin_broadcast` for mesh-wide
    `subscribe`/`expose`/`unsubscribe`/`unexpose` propagation.

    Stamps `msg.data["rmark"]` with `router.eid`:

    - if `router.eid` is already present, this router has already
      processed `msg` (the flood looped back to it through a cycle in
      the mesh graph); the function returns `False` *without*
      re-broadcasting, so the caller must not reapply the corresponding
      local table update either;
    - otherwise `router.eid` is appended, `admin_broadcast` is called to
      flood `msg` to this router's neighbors, and `True` is returned so
      the caller proceeds to update its local `subscribers`/`exposers`
      tables.

    Together with the per-link `touch` marker used by
    `admin_broadcast`, this guarantees a command reaches every router in
    the mesh exactly once, regardless of topology (tree, ring, or
    arbitrary graph).
    """
    rmark = msg.data.setdefault("rmark", [])
    if router.eid in rmark:
        # Already traversed, do not touch the local tables.
        return False

    rmark.append(router.eid)
    await admin_broadcast(router, twin, msg)
    return True


def ismultipath(router) -> bool:
    """Return `True` if `router` should maintain mesh routing tables
    (`subscribers`/`exposers`) at all, i.e. it could have more than one
    outstanding route: it is a real broker or a pool component, as
    opposed to a plain single-link component.

    Currently always returns `True`; kept as an explicit gate (and
    extension point) around every table update in `admin_command` so the
    optimization for non-routing components can be reintroduced without
    touching call sites.
    """
    return True


async def reactive(router, twin, status: bool):
    logger.debug("[%s] reactive: %s", twin, status)
    twin.isreactive = status
    if status:
        await router.inbox.put(rp.SendDataAtRest(twin))


def set_private_topic(twin, topic):
    logger.debug("[%s] set private topic [%s]", twin, topic)
    router = twin.router
    if topic not in router.private_topics:
        router.private_topics[topic] = {}


def set_public_topic(twin, topic):
    logger.debug("[%s] set public topic [%s]", twin, topic)
    router = twin.router
    router.private_topics.pop(topic, None)


def authorize(twin, cid, topic):
    router = twin.router
    if topic not in router.private_topics:
        set_private_topic(twin, topic)

    router.private_topics[topic][cid] = True


def unauthorize(twin, cid, topic):
    router = twin.router
    if topic in router.private_topics:
        router.private_topics[topic].pop(cid, None)


async def admin_command(msg: rp.AdminMsg):
    """Handle an administration command received from `msg.twin`, based on
    `msg.data[rp.COMMAND]`.

    This is the single entry point for every mesh-routing state change as
    well as for broker administration (topic privacy, authorization,
    reactivity).

    Mesh-routing-relevant commands:

    - `ADD_INTEREST` / `ADD_IMPL` / `REMOVE_INTEREST` / `REMOVE_IMPL`:
      after an `Router.isauthorized` check, propagate the change
      mesh-wide via `mark_and_broadcast` and, only if that router had not
      already processed this command, apply the corresponding local
      update to `router.subscribers`/`router.exposers` (and
      `twin.msg_from` for subscriptions). This is what lets a publisher
      connected to one broker reach a subscriber connected to another
      broker several hops away: every router on the path ends up with a
      `subscribers`/`exposers` entry pointing towards the neighbor closer
      to the real subscriber/exposer.
    """
    twin = msg.twin
    topic = msg.topic
    if not isinstance(msg.data, dict) or rp.COMMAND not in msg.data:
        logger.warning("admin error: expected cmd property (got: %s)", msg.data)
        await twin.response(rp.STS_ERROR, msg)
        return None

    router = twin.router
    cmd = msg.data[rp.COMMAND]
    if cmd == rp.ADD_IMPL:
        if router.isauthorized(topic, twin):
            if ismultipath(router) and await mark_and_broadcast(
                router, twin, msg
            ):
                add_exposer(router, twin, topic)
        else:
            return await twin.response(rp.STS_ERROR, msg, "unauthorized")
    elif cmd == rp.REMOVE_IMPL:
        if router.isauthorized(topic, twin):
            if ismultipath(router) and await mark_and_broadcast(
                router, twin, msg
            ):
                remove_exposer(router, twin, topic)
        else:
            return await twin.response(rp.STS_ERROR, msg, "unauthorized")
    elif cmd == rp.ADD_INTEREST:
        if router.isauthorized(topic, twin):
            if ismultipath(router) and await mark_and_broadcast(
                router, twin, msg
            ):
                msgfrom = msg.data.get(rp.MSG_FROM, rp.Now)
                add_subscriber(router, twin, topic, msgfrom)
        else:
            return await twin.response(rp.STS_ERROR, msg, "unauthorized")
    elif cmd == rp.REMOVE_INTEREST:
        if router.isauthorized(topic, twin):
            if ismultipath(router) and await mark_and_broadcast(
                router, twin, msg
            ):
                remove_subscriber(router, twin, topic)
        else:
            return await twin.response(rp.STS_ERROR, msg, "unauthorized")
    elif cmd == rp.REACTIVE_CMD:
        await reactive(router, twin, msg.data[rp.STATUS])
    elif cmd == rp.PRIVATE_TOPIC:
        if twin.isadmin():
            logger.debug("[%s] set private topic [%s]", twin, topic)
            set_private_topic(twin, topic)
        else:
            logger.error(
                "[%s] is not admin: unable to elevate [%s] to private",
                twin,
                topic,
            )
            return await twin.response(rp.STS_ERROR, msg)
    elif cmd == rp.PUBLIC_TOPIC:
        if twin.isadmin():
            logger.debug("[%s] set public topic [%s]", twin, topic)
            set_public_topic(twin, topic)
        else:
            logger.error(
                "[%s] is not admin: unable to lower [%s] to public", twin, topic
            )
            return await twin.response(rp.STS_ERROR, msg)
    elif cmd == rp.AUTHORIZE:
        cid = msg.data[rp.CID]
        if twin.isadmin():
            authorize(twin, cid, topic)
        else:
            logger.error(
                "[%s] is not admin: unable to authorize [%s] to [%s]",
                twin,
                cid,
                topic,
            )
            return await twin.response(rp.STS_ERROR, msg)
    elif cmd == rp.UNAUTHORIZE:
        cid = msg.data[rp.CID]
        if twin.isadmin():
            unauthorize(twin, cid, topic)
        else:
            logger.error(
                "[%s] is not admin: unable to unauthorize [%s] to [%s]",
                twin,
                cid,
                topic,
            )
            return await twin.response(rp.STS_ERROR, msg)

    await twin.response(rp.STS_OK, msg)
