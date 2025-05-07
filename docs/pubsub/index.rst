Publish/Subscribe
=================

The publisher
-------------

.. tab:: sync

    .. code-block:: python

         import rembus

         rb = rembus.node("mynode")
         rb.publish("mytopic", "mydata")

.. tab:: async

    .. code-block:: python

         import rembus

         async main():
            rb = await rembus.component("mynode")
            await rb.publish("mytopic", "mydata")


The subscriber
--------------


.. tab:: sync

    .. code-block:: python

        import rembus

        def mytopic(data):
            print("Received data:", data)   

        rb = rembus.node("mynode")
        rb.subscribe(mytopic)
        rb.reactive()
        rb.wait()

.. tab:: async

    .. code-block:: python

        import rembus
        
        def mytopic(data):
            print("Received data:", data)
        
        async main():
            rb = await rembus.component("mynode")
            await rb.subscribe(mytopic)
            await rb.reactive()
            await rb.wait()

.. toctree::
   :maxdepth: 2
