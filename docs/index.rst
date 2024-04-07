.. Rembus.py documentation master file, created by
   sphinx-quickstart on Fri Apr  5 16:51:28 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Rembus API for Python
=====================================

Rembus is both a cross-language protocol specification and a middleware
for distributed applications using RPC and Pub/Sub communication styles.

The standard setup requires a broker that decouples components.
The fastest way is to pull and run a docker image:

.. code-block:: shell

   docker pull rembus/caronte
   docker run --rm -p 8000:8000 -t rembus/caronte

Otherwise, if you have `Julia <https://julialang.org>`_ installed,
you may download the `Rembus.jl <https://github.com/cardo-org/Rembus.jl>`_ 
package and run caronte:

.. code-block:: shell

   git clone https://github.com/cardo-org/Rembus.jl
   cd Rembus.jl
   bin/caronte

Rembus.py provide a synchronous API:

.. code-block:: python

   import rembus.sync as rembus

   rb = rembus.component()
   rb.rpc("version")


and an asynchronous API:

.. code-block:: python

   import rembus

   async main():
      rb = await rembus.component()
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
