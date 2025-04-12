.. Rembus documentation master file, created by
   sphinx-quickstart on Sat Apr 12 11:04:55 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Rembus
=====================================

Rembus is both a cross-language protocol specification and a middleware
for distributed applications using RPC and Pub/Sub communication styles.

The standard setup requires a broker that decouples components.
The fastest way is to pull and run a docker image:

.. code-block:: shell

   docker pull rembus/broker
   docker run --rm -p 8000:8000 -t rembus/broker

Otherwise, if you have `Julia <https://julialang.org>`_ installed,
you may download the `Rembus.jl <https://github.com/cardo-org/Rembus.jl>`_ 
package and run caronte:

.. code-block:: shell

   git clone https://github.com/cardo-org/Rembus.jl
   cd Rembus.jl
   bin/broker

Rembus.py provide a synchronous API:

.. code-block:: python

   import rembus

   rb = rembus.node("mynode")
   rb.rpc("version")


and an asynchronous API:

.. code-block:: python

   import rembus

   async main():
      rb = await rembus.component("mynode")
      await rb.rpc("version")


.. toctree::
   :maxdepth: 2
   :caption: Contents:
   
   source/modules.rst

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`