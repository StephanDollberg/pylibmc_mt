import pylibmc
import multiprocessing

# multithreading wrapper for pylibmc to hide pooling and provide a clean interface
class pylibmc_mt(): 
    def __init__(self, servers, binary=False, behaviors=None, username=None, password=None):
        self.behaviors = behaviors
        self.mc_client = pylibmc.Client(servers=servers, binary=binary, behaviors=behaviors, 
                                            username=username, password=password)
        self.pool = pylibmc.ClientPool(mc=self.mc_client,n_slots=multiprocessing.cpu_count() * 2) # profiling needed, IO dependant 

    def __contains__(self, key):
        with self.pool.reserve() as mc:
            return key in mc

    def __getitem__(self, key): 
        return self.get(key=key)

    def __setitem__(self, key, value):
        return self.set(key=key, value=value)

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
                return mc.get_multi(keys, key_prefix)

    def set(self, key, value, time=0, min_compress_len=0):
        with self.pool.reserve() as mc:
            return mc.set(key, value, time, min_compress_len)

    def set_multi(self, mapping, time=0, key_prefix=None):
        with self.pool.reserve() as mc:
            if key_prefix == None:
                return mc.set_multi(mapping, time)
            else:
                return mc.set_multi(mapping, time, key_prefix)

    def delete(self, key, time=0):
        with self.pool.reserve() as mc:
            return mc.delete(key,time)

    def replace(self, value, time=0, min_compress_len=0):
        with self.pool.reserve() as mc:
            return mc.replace(value, time, min_compress_len)

    def append(self, key, value):
        with self.pool.reserve() as mc:
            return mc.append(key, value)

    def incr(self, key, delta=1):
        with self.pool.reserve() as mc:
            return mc.incr(key, delta)

    def decr(self, key, delta=1):
        with self.pool.reserve() as mc:
            return mc.decr(key, delta)

    def delete_multi(keys, time=0, key_prefix=None):
        with self.pool.reserve() as mc:
            if key_prefix == None:
                return mc.delete_multi(keys, time)
            else:
                return mc.delete_multi(keys, time, key_prefix)

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
