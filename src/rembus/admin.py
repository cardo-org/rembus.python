
import logging
import rembus.protocol as rp

logger = logging.getLogger(__name__)

async def admin_command(msg:rp.AdminMsg):
    """Handle admin commands"""
    twin = msg.twin
    topic = msg.topic
    if not isinstance(msg.data, dict) or rp.COMMAND not in msg.data:
        logger.warning("admin error: %s", msg.data)
        await twin.response(rp.STS_ERROR, msg)
    
    router = twin.router
    cmd = msg.data[rp.COMMAND]
    if cmd == rp.ADD_IMPL:
        logger.debug(
            "[%s] adding [%s] exposer for topic [%s]", router, twin, topic
        )
        if topic not in router.exposers:
            router.exposers[topic] = []
        
        if twin not in router.exposers[topic]:
            router.exposers[topic].append(twin)

    elif cmd == rp.REMOVE_IMPL:
        if topic in router.exposers:
            if twin in router.exposers[topic]:
                router.exposers[topic].remove(twin)

    elif cmd == rp.ADD_INTEREST:
        logger.debug(
            "[%s] adding [%s] subscriber for topic [%s]", router, twin, topic
        )
        if topic not in router.subscribers:
            router.subscribers[topic] = []
        
        if twin not in router.subscribers[topic]:
            router.subscribers[topic].append(twin)

    elif cmd == rp.REMOVE_INTEREST:
        if topic in router.subscribers:
            if twin in router.subscribers[topic]:
                router.subscribers[topic].remove(twin)

    await twin.response(rp.STS_OK, msg)