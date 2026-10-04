"""Test the rembus mesh network feature.

A node that is both a connected component and a broker (started with a
listening ``port``) advertises its reachable protocols/ports through the
``meta`` field of the Identity/Attestation handshake. The upstream broker
collects this information into ``router.network`` as a list of
:class:`~rembus.core.Node`, enabling discovery of the mesh topology.

The tests below also exercise the actual routing/dispatch logic
(``rembus.admin.mark_and_broadcast``/``admin_broadcast`` and
``rembus.router.Router.find_implementor``): a chain of brokers
``pub/rpc_caller -> A -> meshb -> subc/exposer`` forwards Pub/Sub messages
and RPC requests hop by hop, exactly as described in
``docs/src/mesh_routing.md`` of Rembus.jl.
"""

import time
import rembus


def wait_for(predicate, timeout=2.0, interval=0.05):
    """Poll `predicate` until it is truthy or the timeout expires."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def test_mesh_node_advertises_endpoint():
    """A node acting also as a broker is registered in the mesh topology."""
    server = rembus.node(port=8991)
    mesh_node = rembus.node("ws://:8991/meshnode", port=8992)

    wait_for(lambda: len(server.router.network) == 1)

    assert len(server.router.network) == 1
    peer = server.router.network[0]
    assert peer.cid == "meshnode"
    assert peer.protocol == "ws"
    assert peer.port == 8992
    assert peer.status == "up"

    mesh_node.close()
    server.close()


def test_plain_client_does_not_join_mesh():
    """A named component that isn't listening is not added to the mesh."""
    server = rembus.node(port=8993)
    plain = rembus.node("ws://:8993/plainclient")

    # give the handshake some time to complete
    time.sleep(0.2)

    assert server.router.network == []

    plain.close()
    server.close()


def service(x, y):
    """RPC handler exposed on the leaf node of the mesh chain."""
    return x + y


def test_mesh_pubsub_forwarding():
    """A publisher on broker A reaches a subscriber connected to a
    second broker (meshb) chained behind A, with no direct link between
    them: the ``SUBSCRIBE_CMD`` admin command issued by the subscriber is
    flooded upstream (`mark_and_broadcast`/`admin_broadcast`) so that A's
    `subscribers` table routes the message towards `meshb`, which then
    delivers it locally.
    """
    received = []

    def on_temperature(value):
        received.append(value)

    broker_a = rembus.node(name="pubsub_a", port=9201)
    meshb = rembus.node("ws://:9201/meshb", name="pubsub_meshb", port=9202)
    # The uplink twin must be reactive for broker A to forward messages
    # onto it, exactly like any other subscribing component.
    meshb.reactive()

    subc = rembus.node("ws://:9202/subc", name="pubsub_subc")
    subc.subscribe(on_temperature, topic="temperature")
    subc.reactive()

    pub = rembus.node("ws://:9201/pub", name="pubsub_pub")

    try:
        wait_for(lambda: "temperature" in broker_a.router.subscribers)
        wait_for(lambda: "temperature" in meshb.router.subscribers)

        pub.publish("temperature", 21.5)

        assert wait_for(lambda: received == [21.5])
    finally:
        pub.close()
        subc.close()
        meshb.close()
        broker_a.close()


def test_mesh_rpc_forwarding():
    """An RPC request issued against broker A is routed to an exposer
    connected to the chained broker `meshb`: `EXPOSE_CMD` propagation
    populates A's `exposers` table with the twin towards `meshb`, and
    `Router.find_implementor` selects it hop by hop.
    """
    broker_a = rembus.node(name="rpc_a", port=9203)
    meshb = rembus.node("ws://:9203/meshb", name="rpc_meshb", port=9204)
    meshb.reactive()

    expo = rembus.node("ws://:9204/expo", name="rpc_expo")
    expo.expose(service)

    caller = rembus.node("ws://:9203/caller", name="rpc_caller")

    try:
        wait_for(lambda: "service" in broker_a.router.exposers)

        assert caller.rpc("service", 2, 3) == 5
    finally:
        caller.close()
        expo.close()
        meshb.close()
        broker_a.close()


def test_reconnect_resyncs_exposed_topics():
    """After a client's connection to its broker drops and reconnects,
    `Twin.setup` replays its exposed topics via `SETUP_CMD` so the
    broker's `exposers` routing table is resynchronized without the user
    having to call `expose()` again (see `rembus.admin.admin_command`'s
    `SETUP_CMD` branch and `Twin._reconnect`).
    """
    broker = rembus.node(name="reconnect_broker", port=9205)
    client = rembus.node(
        "ws://:9205/reconnect_client", name="reconnect_client"
    )
    client.expose(service)

    caller = rembus.node("ws://:9205/reconnect_caller", name="reconnect_caller")

    try:
        wait_for(lambda: "service" in broker.router.exposers)
        assert caller.rpc("service", 2, 3) == 5

        # Simulate a transient network failure: force-close the
        # client's own socket (without calling client.close()) so its
        # receiver loop detects the drop and triggers `_reconnect`.
        client.exec(client._rb.socket.close)

        # Give the reconnect loop time to reconnect and replay SETUP_CMD.
        wait_for(
            lambda: (
                "service" in broker.router.exposers
                and any(
                    t.isopen() for t in broker.router.exposers["service"]
                )
            ),
            timeout=5.0,
        )

        assert caller.rpc("service", 4, 5) == 9
    finally:
        caller.close()
        client.close()
        broker.close()
