import logging
import rembus.protocol as rp
#import rembus.db as rdb

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

    twin.msg_from[topic] = msgfrom
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

async def admin_command(msg: rp.AdminMsg):
    """Handle admin commands"""
    twin = msg.twin
    topic = msg.topic
    if not isinstance(msg.data, dict) or rp.COMMAND not in msg.data:
        logger.warning("admin error: expected cmd property (got: %s)", msg.data)
        await twin.response(rp.STS_ERROR, msg)
        return

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
    await twin.response(rp.STS_OK, msg)
