import pylibmc


class pylibmc_mt(): 
    def __init__(self, servers, binary=False, behaviors=None, username=None, password=None):
        self.mc = pylibmc.Client(servers=servers, binary=binary, behaviors=behaviors, username=username, password=password)
        self.pool = pylibmc.ClientPool(mc=self.mc,n_slots=4)

    
