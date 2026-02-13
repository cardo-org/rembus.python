.. Rembus documentation master file, created by
   sphinx-quickstart on Sat Apr 12 11:04:55 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Rembus
======

Rembus is both a cross-language protocol specification and a middleware
for distributed applications using RPC and Pub/Sub communication styles.

The standard setup requires a broker that links and decouples components.
The fastest way is to pull and run a docker image for the broker:


.. tab:: ws 

      .. code-block:: shell

         docker pull rembus/broker:1.2.3
         docker run --rm \
                    -p 8000:8000 \
                    -e REMBUS_DIR=/etc/rembus \
                    -v $HOME/.config/rembus:/etc/rembus \
                    -t rembus/broker

.. tab:: wss

      .. code-block:: shell

         docker pull rembus/broker
         docker run --rm \
                    -p 8000:8000 \
                    -e REMBUS_DIR=/etc/rembus \
                    -v $HOME/.config/rembus:/etc/rembus \
                    -t rembus/broker -s

      The Rembus broker expects the private key and certificate to be named `rembus.key` and
      `rembus.crt` respectively and to be in the host directory
      `$HOME/.config/rembus/keystore`.

Otherwise, if you have `Julia <https://julialang.org>`_ installed,
you may download the `Rembus.jl <https://github.com/cardo-org/Rembus.jl>`_ 
package and run caronte:

.. code-block:: shell

   git clone https://github.com/cardo-org/Rembus.jl
   cd Rembus.jl
   bin/broker

Rembus provides both a synchronous and an asynchronous API.


.. tab:: sync

   .. code-block:: python

         import rembus as rb

         cli = rb.node("mynode")
         cli.rpc("version")

.. tab:: async

   .. code-block:: python

         import rembus as rb

         async main():
            cli = await rb.component("mynode")
            await cli.rpc("version")

`rembus.component` returns a coroutine object and `rembus.node` returns a blocking handle
for interacting with the broker.


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   reference/index
   pubsub/index
   rpc/index

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`