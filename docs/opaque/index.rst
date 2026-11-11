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

It is opt-in: the standard ``Register`` keeps working and ``Rekey`` still uses
the standard pin proof.

Usage
-----

Enable the tenant (broker host)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The administrator stores an OPAQUE record for the tenant with
``tools/tenant.py``. The command must run where the broker directory is
readable, because it creates (if missing) and uses the broker *server setup*
file:

.. code-block:: console

   $ python tools/tenant.py --opaque -s 11223344 .        # default tenant
   $ python tools/tenant.py --opaque -s 11223344 acme.com # tenant "acme.com"
   $ python tools/tenant.py --opaque -s 11223344 --broker mybroker acme.com

Running the command again for the same tenant replaces its record (pin
change). ``tools/tenant.py -d TENANT`` removes both the plain secret and the
OPAQUE record. A running broker loads the records at startup, so restart it
after changing them.

The same tenant can have a plain secret and an OPAQUE record: the protocol
used is chosen by the component.

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

* ``tenant [acme.com] not enabled``: no OPAQUE record for the tenant;
* ``wrong tenant/pin``: the pin does not match the tenant record (detected by
  the component, when the broker proof does not verify, or by the broker);
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
   * - Password file (table ``tenant_opaque``: ``name``, ``tenant``,
       ``password_file``)
     - broker, database
     - OPAQUE record of the tenant: client public key and the encrypted
       *envelope*; contains no pin and no pin hash
   * - Pin
     - component only
     - never leaves the component

The OPRF key of a tenant is derived from the seed and the OPAQUE credential
identifier, so no per-tenant secret has to be stored. The OPAQUE *username* is
the tenant name: one record serves all the components of the tenant.

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
   Opaque(stage=1, cid, data=KE1)        ----->   tenant = tenant_of(cid)
                                                  record = tenant_opaque[tenant]
                                                  KE2, state = server_login_start(
                                                       setup, record, KE1, tenant)
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
* The existence of a tenant record is revealed by the stage 1 error
  (``tenant [x] not enabled``), as in the standard registration.
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
   * - ``tenant_opaque`` table and ``Router.opaque_tenants``
     - ``rembus/db.py``, ``rembus/router.py``
   * - administration tool
     - ``tools/tenant.py``
