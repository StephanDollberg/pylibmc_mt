# (C) Copyright Stephan Dollberg 2012-2013. Distributed under the Boost
# Software License, Version 1.0. (See accompanying file
# LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

import pylibmc
import multiprocessing

# multithreading wrapper for pylibmc to hide pooling and provide a clean interface
class Client(): 
    def __init__(self, servers, binary=False, behaviors=None, username=None, password=None):
        self.behaviors = behaviors
        # we are sorting server names to guarantee equal key hashing across clients
        servers.sort()
        self.mc_client = pylibmc.Client(servers=servers, binary=binary, behaviors=behaviors, 
                                            username=username, password=password)
        # profiling needed, IO dependent
        self.pool = pylibmc.ClientPool(mc=self.mc_client,n_slots=multiprocessing.cpu_count() * 2) 

    def __contains__(self, key):
        with self.pool.reserve() as mc:
            return key in mc

    def __getitem__(self, key): 
        return self.get(key)

    def __setitem__(self, key, value):
        return self.set(key, value)

    def __delitem__(self, key):
        return self.delete(key)

    def __str__(self):
        return str(self.mc_client)

    def __repr__(self):
        return repr(self.mc_client)

    def get(self, key):
        with self.pool.reserve() as mc:
            return mc.get(key)

    def get_multi(self, keys, key_prefix=None):
        with self.pool.reserve() as mc:
            if key_prefix == None:
                return mc.get_multi(keys)
            else:
                return mc.get_multi(keys, key_prefix = key_prefix)

    def set(self, key, value, time=0, min_compress_len=0):
        with self.pool.reserve() as mc:
            return mc.set(key, value, time = time, min_compress_len = min_compress_len)

    def set_multi(self, mapping, time=0, key_prefix=None):
        with self.pool.reserve() as mc:
            if key_prefix == None:
                return mc.set_multi(mapping, time)
            else:
                return mc.set_multi(mapping, time, key_prefix = key_prefix)

    def add(self, key, value, time=0, min_compress_len=0):
        with self.pool.reserve() as mc:
            return mc.add(key, value, time, min_compress_len = min_compress_len)

    def add_multi(self, mapping, time=0, key_prefix=None):
        with self.pool.reserve() as mc:
            if key_prefix:
                return mc.add_multi(mapping, time, key_prefix = key_prefix)
            else:
                return mc.add_multi(mapping, time)

    def replace(self, value, time=0, min_compress_len=0):
        with self.pool.reserve() as mc:
            return mc.replace(value, time = time, min_compress_len = min_compress_len)

    def append(self, key, value):
        with self.pool.reserve() as mc:
            return mc.append(key, value)

    def prepend(self, key, value):
        with self.pool.reserve() as mc:
            return mc.prepend(key, value)

    def incr(self, key, delta=1):
        with self.pool.reserve() as mc:
            return mc.incr(key, delta)

    def incr_multi(self, keys, delta=1, key_prefix=''):
        with self.pool.reserve() as mc:
            return mc.incr_multi(keys, delta = delta, key_prefix = key_prefix)

    def decr(self, key, delta=1):
        with self.pool.reserve() as mc:
            return mc.decr(key, delta = delta)

    def gets(self, key):
        with self.pool.reserve() as mc:
            return mc.gets(key)

    def cas(self, key, value, time=0):
        with self.pool.reserve() as mc:
            return mc.cas(key, value, time = time)

    def delete(self, key):
        with self.pool.reserve() as mc:
            return mc.delete(key)

    def delete_multi(self, keys, time=0, key_prefix=None):
        with self.pool.reserve() as mc:
            if key_prefix == None:
                return mc.delete_multi(keys, time)
            else:
                return mc.delete_multi(keys, time, keyprefix = key_prefix)

    def flush_all(self):
        with self.pool.reserve() as mc:
            return mc.flush_all()

    def get_stats(self):
        with self.pool.reserve() as mc:
            return mc.get_stats()

    def clone(self): # don't know if this makes sense
        with self.pool.reserve() as mc:
            return mc.clone()

    def get_behaviors(self):
        with self.pool.reserve() as mc:
            return self.mc.get_behaviors()

    def set_behaviors(self, behaviors):
        with self.pool.reserve() as mc:
            return mc.set_behaviors(behaviors)
