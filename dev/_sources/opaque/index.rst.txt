OPAQUE registration
===================

The standard ``Register`` operation authorizes a component with the tenant
secret (``pin``): the component sends ``SHA256(CBOR([pin, cid]))`` and the
broker, which must be able to recompute it, stores the pin in clear text in
the ``tenant`` table.

OPAQUE registration (`RFC 9807 <https://www.rfc-editor.org/rfc/rfc9807>`_, an
*asymmetric password-authenticated key exchange*) removes that weakness:

* the broker stores neither the ``pin`` nor a hash of it;
* the ``pin`` is never sent, not even in a form that can be brute-forced
  offline from a captured exchange;
* a captured exchange cannot be replayed;
* the component also verifies that it is talking to the broker that holds the
  tenant record.

It is opt-in: the standard ``Register`` keeps working. ``Rekey`` can use
either the standard pin proof or OPAQUE (``rekey(pin, opaque=True)``), as the
mode of the tenant requires.

A tenant is in one mode only: ``pin-proof`` or ``opaque-only``. In an
``opaque-only`` tenant the broker holds no pin at all (tenant or ``cid``) and a
standard pin proof always fails. See :doc:`/tenants/index` for the mode rules.

Usage
-----

Enable the tenant (broker host)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The administrator stores an OPAQUE record for the tenant with
``rembus-secure``. The command must run where the broker directory is
readable, because it creates (if missing) and uses the broker *server setup*
file:

.. code-block:: console

   $ rembus-secure --opaque -s 11223344 .        # default tenant
   $ rembus-secure --opaque -s 11223344 acme.com # tenant "acme.com"
   $ rembus-secure --opaque -s 11223344 --broker mybroker acme.com

Running the command again for the same tenant replaces its record (pin
change). ``rembus-secure -d TENANT`` removes the tenant secret. A running
broker loads the records at startup, so restart it after changing them.

A tenant cannot hold both a plain secret and an OPAQUE record: adding one of
the other mode raises ``ValueError`` until the old one is removed.

Record for a single component
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A component can have its own pin, stored as its own OPAQUE record. The
record is bound to the identifier ``cid:<cid>``, so it cannot be used for the
tenant or for another ``cid``. The tenant record is used for the ``cid`` s
without one; a ``cid`` with its own record never accepts the tenant pin:

.. code-block:: console

   $ rembus-secure --cid --opaque -s 99887766 sensor1.acme.com
   $ rembus-secure --cid -d sensor1.acme.com    # back to the tenant record

From Python: ``rembus.add_cid_opaque(cid, pin)`` and ``rembus.remove_cid(cid)``.
``Unregister`` removes the key and the record of the ``cid``.

Register a component
^^^^^^^^^^^^^^^^^^^^

Pass ``opaque=True``. The component name selects the tenant as described in
:doc:`/tenants/index`:

.. code-block:: python

   import rembus as rb

   rb.register("ws://broker:8338/sensor1", "11223344", opaque=True)
   rb.register("ws://broker:8338/sensor1.acme.com", "11223344", opaque=True)

   # same options of the standard registration
   rb.register(
       "ws://broker:8338/sensor2", "11223344", rb.SIG_ECDSA, opaque=True
   )

   # authenticated connection as usual afterwards
   cli = rb.node("ws://broker:8338/sensor1")

With an already connected node, ``node.register(rid, pin, scheme, opaque=True)``
(sync API) and ``await twin.register(rid, pin, scheme, opaque=True)`` (async
API) behave the same.

Errors
^^^^^^

``RembusError`` is raised with the same reasons of the standard registration:

* ``wrong tenant/pin``: no secret applies to the ``cid`` or the pin does not
  match the resolved record (detected by the component, when the broker proof
  does not verify, or by the broker). The reply does not tell which;
* ``[sensor1] not available`` (``STS_NAME_ALREADY_TAKEN``): the name is
  already registered;
* invalid public key (type mismatch, RSA smaller than 2048 bits, ECDSA curve
  other than P-256);
* ``unexpected opaque message``: stage 2 without a preceding stage 1 of the
  same ``cid`` on the same connection.

On any failure no key is stored and no private key is saved locally.

Operational notes
^^^^^^^^^^^^^^^^^

* The server setup is the file ``opaque_setup`` (mode ``0600``) in the broker
  directory (``<rembus dir>/<broker name>/``), created on first use. It holds
  the OPRF seed and the server keypair. **Back it up and keep it secret**: if
  it is lost every OPAQUE record becomes unusable and the tenants must be
  enrolled again; if it leaks together with the database, pins become
  brute-forceable offline (still with a slow key derivation per guess).
* Do not copy the database to another broker without the matching
  ``opaque_setup``.
* The dependency is `opaque-ke-py <https://pypi.org/project/opaque-ke-py/>`_,
  Python bindings of the Rust ``opaque-ke`` crate.

Design
------

Roles and data
^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1

   * - Item
     - Where
     - Content
   * - Server setup (``opaque_setup``)
     - broker, file mode 0600
     - OPRF seed and server long-term keypair; one per broker
   * - Password file (tables ``tenant_opaque``: ``name``, ``tenant``,
       ``password_file``; ``cid_opaque``: ``name``, ``cid``,
       ``password_file``)
     - broker, database
     - OPAQUE record of the tenant or of a ``cid``: client public key and the
       encrypted *envelope*; contains no pin and no pin hash
   * - Pin
     - component only
     - never leaves the component

The OPRF key of a record is derived from the seed and the OPAQUE credential
identifier, so no per-record secret has to be stored. The identifier is the
tenant name for a tenant record and ``cid:`` followed by the ``cid`` for a
``cid`` record (``:`` is not allowed in a ``cid``, so there are no
collisions). One tenant record serves all the components of the tenant that
have no record of their own.

Enrollment (administrator)
^^^^^^^^^^^^^^^^^^^^^^^^^^

``rembus.opaque.enroll`` runs the OPAQUE registration locally: the client
half blinds the pin, the server half evaluates it with the tenant OPRF key
and the client half finalizes the record, which is stored in
``tenant_opaque``. It is done in-process because the administrator already
holds the pin and the server setup; no OPAQUE registration traffic crosses the
network.

Registration of a component
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The component runs an OPAQUE *login* against the tenant record and binds the
public key to register to the resulting session with a MAC:

.. code-block:: text

   component                                      broker
   ---------                                      ------
   KE1 = client_login_start(pin)
   Opaque(stage=1, cid, data=KE1)        ----->   record = cid_opaque[cid], else
                                                  tenant_opaque[tenant_of(cid)],
                                                  else a fake record
                                                  KE2, state = server_login_start(
                                                       setup, record, KE1,
                                                       "cid:"+cid | tenant)
                                         <-----   Response(OK, KE2)
                                                  (state kept on the connection)
   KE3, session_key = client_login_finish(
        pin, state, KE2)                          # fails on wrong pin or
                                                  # unauthentic broker
   mac = HMAC(session_key, "rembus-register"
        || SHA256(cid || key_type || pubkey))
   Opaque(stage=2, cid, data=KE3,
          pubkey, key_type, mac)         ----->   session_key = server_login_finish(
                                                       state, KE3)
                                                  verify mac
                                                  not already registered?
                                                  validate and store pubkey
                                         <-----   Response(OK)
   save private key

Properties and decisions:

* **Pin knowledge is proven by the login**; the MAC under the session key
  makes the key to register inseparable from that proof: a man in the middle
  cannot swap the public key, and a captured KE3 is useless without the
  session key.
* The server state is stored in ``twin.handler["opaque"]`` together with the
  ``cid`` and is **removed when stage 2 arrives**, whatever its outcome, so
  each stage 1 allows exactly one finalization; stage 2 with a different
  ``cid`` is rejected.
* The private key is generated before the exchange but saved locally only
  after the broker accepts the registration.
* A wrong pin is reported as ``wrong tenant/pin``: the component cannot
  recover the envelope, and cannot tell a bad pin from a broker that does not
  hold the real record.
* A tenant, a ``cid`` or the tenant of a ``cid`` without record all get the same
  stage 1 reply (a fake record is used), so the stage 1 does not reveal
  whether a record exists; every failure surfaces at stage 2.
* The login state is per connection and not persisted: a broker restart
  between the two stages makes the component retry.

Wire format
^^^^^^^^^^^

Packet type ``TYPE_OPAQUE = 8``. CBOR array::

   [0x08 | flags, id, cid, stage, data, pubkey, key_type, mac]

.. list-table::
   :header-rows: 1

   * - Field
     - Stage 1
     - Stage 2
   * - ``stage``
     - ``1`` (``OPAQUE_START``)
     - ``2`` (``OPAQUE_FINISH``)
   * - ``data``
     - credential request (KE1)
     - credential finalization (KE3)
   * - ``pubkey``
     - ``null``
     - PEM public key
   * - ``key_type``
     - ``null``
     - ``1`` RSA, ``2`` ECDSA
   * - ``mac``
     - ``null``
     - MAC described above

The stage 1 response is a standard ``Response`` with status ``0`` and the
credential response (KE2) as data; the stage 2 response has status ``0`` and
no data, or an error status with the reason as data.

Over JSON-RPC the packet is a request whose ``method`` is the ``cid``:

.. code-block:: json

   {"rembus": "1.0", "id": "123", "method": "sensor1",
    "params": {"type": 8, "stage": 2, "data": "BASE64",
               "key_val": "BASE64", "key_type": 1, "mac": "BASE64"}}

Byte values are base64 text; ``key_val``, ``key_type`` and ``mac`` are present
only at stage 2. Elements after the eighth of the CBOR array are forwarded
unchanged, like in the other packets.

Implementation map
^^^^^^^^^^^^^^^^^^

.. list-table::
   :header-rows: 1

   * - Piece
     - Location
   * - ``OpaqueMsg``, ``TYPE_OPAQUE``, ``OPAQUE_START``, ``OPAQUE_FINISH``
     - ``rembus/protocol.py``
   * - server setup, enrollment, key MAC
     - ``rembus/opaque.py``
   * - broker handler ``Router._opaque_register``
     - ``rembus/router.py``
   * - component side ``Twin.register(..., opaque=True)``
     - ``rembus/twin.py``
   * - ``tenant_opaque`` / ``cid_opaque`` / ``cid_secret`` / ``tenant_mode``
       tables, ``Router._resolve_secret``
     - ``rembus/db.py``, ``rembus/router.py``
   * - administration tool and API (``add_cid``, ``set_tenant_mode``, ...)
     - ``rembus-secure``, ``rembus/tenant.py``
