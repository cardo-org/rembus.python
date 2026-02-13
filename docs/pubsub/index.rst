Publish/Subscribe
=================

The publisher
-------------

.. tab:: sync

    .. code-block:: python

         import rembus as rb

         cli = rb.node("mynode")
         cli.publish("mytopic", "mydata")

.. tab:: async

    .. code-block:: python

         import rembus as rb

         async main():
            cli = await rb.component("mynode")
            await cli.publish("mytopic", "mydata")


The subscriber
--------------


.. tab:: sync

    .. code-block:: python

        import rembus as rb

        def mytopic(data):
            print("Received data:", data)   

        sub = rb.node("mynode")
        sub.subscribe(mytopic)
        sub.reactive()
        sub.wait()

.. tab:: async

    .. code-block:: python

        import rembus as rb
        
        def mytopic(data):
            print("Received data:", data)
        
        async main():
            sub = await rb.component("mynode")
            await sub.subscribe(mytopic)
            await sub.reactive()
            await sub.wait()

.. toctree::
   :maxdepth: 2
