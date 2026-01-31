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
    logger.debug("[%s] adding [%s] exposer for topic [%s]",
                 router, twin, topic)
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
    """Handle admin commands"""
    twin = msg.twin
    topic = msg.topic
    if not isinstance(msg.data, dict) or rp.COMMAND not in msg.data:
        logger.warning(
            "admin error: expected cmd property (got: %s)", msg.data)
        await twin.response(rp.STS_ERROR, msg)
        return None

    router = twin.router
    cmd = msg.data[rp.COMMAND]
    if cmd == rp.ADD_IMPL:
        add_exposer(router, twin, topic)
    elif cmd == rp.REMOVE_IMPL:
        remove_exposer(router, twin, topic)
    elif cmd == rp.ADD_INTEREST:
        add_subscriber(router, twin, topic, msg.data[rp.MSG_FROM])
    elif cmd == rp.REMOVE_INTEREST:
        remove_subscriber(router, twin, topic)
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
                topic
            )
            return await twin.response(rp.STS_ERROR, msg)
    elif cmd == rp.PUBLIC_TOPIC:
        if twin.isadmin():
            logger.debug("[%s] set public topic [%s]", twin, topic)
            set_public_topic(twin, topic)
        else:
            logger.error(
                "[%s] is not admin: unable to lower [%s] to public",
                twin,
                topic
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
                topic
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
                topic
            )
            return await twin.response(rp.STS_ERROR, msg)

    await twin.response(rp.STS_OK, msg)
