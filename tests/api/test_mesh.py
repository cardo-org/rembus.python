"""Test the rembus mesh network feature.

A node that is both a connected component and a broker (started with a
listening ``port``) advertises its reachable protocols/ports through the
``meta`` field of the Identity/Attestation handshake. The upstream broker
collects this information into ``router.network`` as a list of
:class:`~rembus.core.Node`, enabling discovery of the mesh topology.
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
