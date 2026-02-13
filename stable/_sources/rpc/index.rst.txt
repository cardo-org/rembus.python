RPC
=================

Invoke
--------

.. tab:: sync

    .. code-block:: python

         import rembus as rb

         cli = rb.node("mynode")
         cli.rpc("myservice", 2, 3)

.. tab:: async

    .. code-block:: python

         import rembus as rb

         async main():
            cli = await rb.component("mynode")
            await cli.rpc("myservice", 2, 3)


Expose
--------


.. tab:: sync

    .. code-block:: python

        import rembus as rb

        def myservice(x, y):
            return x + y   

        srv = rb.node("mynode")
        srv.expose(myservice)
        srv.wait()

.. tab:: async

    .. code-block:: python

        import rembus as rb
        
        def myservice(x, y):
            return x + y
        
        async main():
            srv = await rb.component("mynode")
            await srv.expose(mytopic)
            await srv.wait()

.. toctree::
   :maxdepth: 2
