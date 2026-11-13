Multi-tenancy
=============

Multi-tenancy provides logical isolation between groups of components that
share the same broker. Components of different tenants can be connected to the
same broker at the same time, but they never see each other's messages or
RPC services.

The feature is compatible with the tenant feature of
`Rembus.jl <https://github.com/cardo-org/Rembus.jl>`_.

Concepts
--------

Tenant
   A named group of components. The tenant is a property of the component
   *name*, not of the connection.

Default tenant
   Components whose name has no domain part belong to the default tenant,
   represented by a single dot (``.``).

Tenant secret (``pin``)
   A shared secret associated to each tenant by the broker administrator. It is
   required to *register* a component of that tenant (see
   :ref:`tenant-registration`).

How the tenant is determined
----------------------------

The tenant is the portion of the component name that follows the **first**
dot (see :func:`rembus.core.domain`):

.. list-table::
   :header-rows: 1

   * - Component name
     - Tenant
   * - ``mynode``
     - ``.`` (default)
   * - ``foo.org``
     - ``org``
   * - ``foo.bar.org``
     - ``bar.org``
   * - ``sensor1.acme.com``
     - ``acme.com``

.. code-block:: python

   from rembus.core import domain

   domain("foo.org")   # "org"
   domain("mynode")    # "."

Isolation rules
---------------

Isolation is enforced by the broker.

Pub/Sub
^^^^^^^
A published message is delivered only to the reactive subscribers belonging to
the **same tenant as the publisher**. Subscribers of other tenants are skipped
even if they subscribed to the same topic.

.. code-block:: python

   import rembus as rb

   sub_org = rb.node("ws://:8338/sub.org")
   sub_org.subscribe(lambda data: print("org:", data), topic="mytopic")
   sub_org.reactive()

   sub_com = rb.node("ws://:8338/sub.com")
   sub_com.subscribe(lambda data: print("com:", data), topic="mytopic")
   sub_com.reactive()

   pub = rb.node("ws://:8338/pub.org")
   pub.publish("mytopic", "hello")   # only ``sub.org`` receives it

RPC
^^^
An RPC request is routed only to *open* exposers belonging to the **same tenant
as the requester**. If the topic is exposed only by components of other tenants
the request fails exactly as if the method was not exposed.

.. code-block:: python

   import rembus as rb

   org = rb.node("ws://:8338/exposer.org")
   org.expose(lambda x: x + 1, topic="myservice")

   cli_org = rb.node("ws://:8338/client.org")
   cli_org.rpc("myservice", 1)       # 2

   cli_com = rb.node("ws://:8338/client.com")
   cli_com.rpc("myservice", 1)       # raises rembus.protocol.RembusError

The tenant filter is applied by every routing policy (``first_up``,
``round_robin`` and ``less_busy``): the policy only chooses among the
exposers of the requester's tenant.

Mesh of brokers
^^^^^^^^^^^^^^^
The tenant of a peer broker is derived from its own name with the same
rules. Requests and messages forwarded through a peer are therefore subject to
the same tenant comparison, as in Rembus.jl.

.. note::

   A broker-side handler (a method or subscriber registered on the broker
   process itself) is not a tenant component and is not filtered by tenant.

.. _tenant-registration:

Registration and tenants
------------------------

Authenticated components are provisioned with
:meth:`rembus.sync.Node.register` (or ``rembus.register``), which generates a
key pair and sends the public key to the broker. The ``pin`` argument must
match the secret configured for the tenant of the component name.

.. code-block:: python

   import rembus as rb

   rb.register("foo.org", "11223344")   # tenant "org", secret "11223344"
   rb.register("bar", "00000000")       # default tenant "."

The broker rejects the registration when:

* the tenant is **not enabled** (no secret configured):
  ``tenant [org] not enabled``;
* the ``pin`` does not match the tenant secret: ``wrong tenant/pin``;
* the component name is already registered (``STS_NAME_ALREADY_TAKEN``).

Single-tenant setups need only the default tenant ``.``. A component without
a domain can be registered only if the ``.`` tenant is configured.

OPAQUE registration
-------------------

The tenant secret can be verified with an OPAQUE exchange so that the broker
stores neither the ``pin`` nor a hash of it: register with
``rb.register(url, "11223344", opaque=True)``. See :doc:`/opaque/index` for
usage and design.

Replacing a component key
-------------------------

An authenticated component can replace its key pair with
:meth:`rembus.sync.Node.rekey` (or ``Twin.rekey`` in the async API). It sends
a ``Rekey`` packet (type ``0x07``) with the same fields as ``Register``; the
``cid`` is always the authenticated name of the component and ``pin`` is the registration secret resolved for the ``cid``
(its own secret, otherwise its tenant's; see below).

.. code-block:: python

   rb = rembus.node("ws://host:8000/foo.org")   # authenticated connection
   rb.rekey("11223344")                          # or rb.rekey(pin, rp.SIG_ECDSA)

On success the broker atomically replaces the stored public key and the new
private key is saved locally; the connection stays open and the new key is
used from the next authentication. On any failure (wrong ``pin``, component
not registered, invalid public key, ``cid`` different from the authenticated
one, unauthenticated sender) the old key is kept and the broker answers with
``STS_GENERIC_ERROR``. With the JSON-RPC encoding the request has the
component name as ``method`` and ``params`` ``{"type": 7, "pin": BASE64,
"key_val": BASE64, "key_type": INT}`` (an absent ``key_type`` defaults to
RSA). As for ``register``, avoid sending it over a plaintext
connection.

Managing tenants
----------------

Tenants and secrets are stored in the ``tenant`` table of the broker
database, one row per ``(broker name, tenant)``:

.. code-block:: sql

   CREATE TABLE IF NOT EXISTS tenant (
       name   TEXT NOT NULL,   -- broker name
       tenant TEXT NOT NULL,
       secret TEXT NOT NULL
   )

The ``rembus-secure`` script manages them:

.. code-block:: shell

   # add or update a tenant (default broker name: "broker")
   rembus-secure org --secret deadbeef
   rembus-secure . --secret deadbeef --broker mybroker

   # remove a tenant
   rembus-secure org --delete

The same operations are available from Python:

.. code-block:: python

   from rembus import add_tenant, remove_tenant

   add_tenant("org", "deadbeef", "broker")
   remove_tenant("org", "broker")

Adding a tenant that already exists replaces its secret.

Secrets per component (``cid``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A registration secret can also be associated with a single ``cid``. For
``Register`` and ``Rekey`` the broker resolves the secret of a ``cid`` as:

1. the secret associated with the ``cid``, if any;
2. otherwise the secret of its tenant;
3. otherwise none: the request fails with ``wrong tenant/pin``.

If a ``cid`` has its own secret the tenant secret is **never** accepted for it,
and a wrong ``cid`` pin is not retried against the tenant pin. The ``cid``
secret authorizes only ``Register`` and ``Rekey`` of that ``cid`` (it does not
change how the component authenticates) and is kept after they succeed. A
secret can be set before the ``cid`` is registered.

.. code-block:: shell

   rembus-secure --cid node1.acme --secret 99887766     # secret of one cid
   rembus-secure --cid node1.acme --delete               # back to the tenant's

.. code-block:: python

   import rembus
   from rembus import add_cid, add_cid_opaque, remove_cid

   add_cid("node1.acme", "99887766")
   remove_cid("node1.acme")

Removing the secret of a ``cid`` does not unregister it: its key stays valid
and a later ``Rekey`` uses the tenant secret. ``Unregister`` removes the key
and the ``cid`` secret together; afterwards the tenant secret can register the
``cid`` again.

Registration mode
^^^^^^^^^^^^^^^^^

A tenant is either ``pin-proof`` or ``opaque-only`` (see :doc:`/opaque/index`),
and every secret of the tenant and of its ``cid`` s must be in that mode.
The mode is derived from the secrets held (all pins: ``pin-proof``; all OPAQUE
records: ``opaque-only``) unless set explicitly:

.. code-block:: shell

   rembus-secure acme --mode opaque-only
   rembus-secure acme --mode derived       # clear the explicit mode

``add_tenant``, ``add_tenant_opaque``, ``add_cid``, ``add_cid_opaque`` and
``set_tenant_mode`` raise ``ValueError`` instead of adding a secret of the
other mode or changing the mode while secrets of the old mode exist. Remove
them first (``remove_tenant`` removes the secret and the explicit mode of the
tenant and keeps the ``cid`` secrets).

The secrets are stored in the tables ``tenant``, ``tenant_opaque``,
``cid_secret``, ``cid_opaque`` and ``tenant_mode``.

.. important::

   The broker loads the tenants once, at startup, when it is started with a
   listening port. Restart the broker after changing the table. Stop the broker
   before running the script too, because the database is attached by a
   single process at a time.

Unlike Rembus.jl, the ``tenants.json`` file based configuration is not
supported: tenants are read only from the broker database.

Complete example
----------------

.. code-block:: shell

   # 1. configure the tenants (broker stopped)
   rembus-secure org --secret orgsecret
   rembus-secure com --secret comsecret

   # 2. start the broker
   brokerd

.. code-block:: python

   import rembus as rb

   # 3. provision components
   rb.register("exposer.org", "orgsecret")
   rb.register("client.org", "orgsecret")
   rb.register("client.com", "comsecret")

   # 4. components of different tenants cannot interact
   srv = rb.node("exposer.org")
   srv.expose(lambda x: x * 2, topic="double")

   rb.node("client.org").rpc("double", 4)   # 8
   rb.node("client.com").rpc("double", 4)   # RembusError: method unavailable

Summary of behaviours
---------------------

.. list-table::
   :header-rows: 1

   * - Operation
     - Cross-tenant behaviour
   * - ``publish``
     - not delivered to subscribers of other tenants
   * - ``rpc``
     - other tenants' exposers are never selected (method unavailable)
   * - ``register``
     - requires the secret of the component's tenant; tenant must be enabled
   * - ``rekey``
     - requires an authenticated component and the secret of its tenant
