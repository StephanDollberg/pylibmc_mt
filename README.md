pylibmc_mt is a multithreading wrapper for the memcached python library pylibmc. 

It encapsulates pooling such that you can operate on your client object from multiple threads.

The syntax is completely equal to pylibmc so that you don't have to refactor anything.

    # somewhere
    import pylibmc_mt
    mc = pylibmc_mt.Client(['localhost'])

    # somewhere else in your class
    @route('/getkey')
    def get_key(key):
        return mc.get(key) 

The example shows that it's safe to operate on the memcached object even on a multithreaded server with concurrent calls to `get_key`.

One thing to note is that pylibmc_mt sorts the given server names to guarantee equal hashing across all clients.
