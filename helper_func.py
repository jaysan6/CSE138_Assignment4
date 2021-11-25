####
#
#
# THIS HELPER FUNCTION FILE CONTAINS IMPLEMENTATIONS FOR CAUSAL BROADCAST OPERATIONS
# AND FUNCTIONS RELEVANT TO CAUSALITY
#

#
####
from hashlib import sha1

## sharding
def key_to_shard(key, num_shard):
    hash_str = sha1(key.encode('utf8'))
    val = hash_str.hexdigest()
    return int(val, 16) % num_shard
    
#checks if cond 1 of causal broadcast is violated
def list_less1(VC_message, VC_local, p):
    return (int(VC_message[p]) == int(VC_local[p])+1)

#checks if cond 2 of causal broadcast is violated
def list_less2(VC1,  VC2, sender):
    shared_processes = VC1.keys() & VC2.keys() - {sender}
    for p in shared_processes:
        if(int(VC1[p]) > int(VC2[p])):
            return False
    return True

def concurrent(VC1, VC2):
    return (not happens_before(VC1, VC2)) and (not happens_before(VC2, VC1))

def equal(VC1, VC2):
    shared_processes = VC1.keys() & VC2.keys()
    for p in shared_processes:
        if VC1[p] != VC2[p]:
            return False
    return True

def happens_before(VC1, VC2):
    shared_processes = VC1.keys() & VC2.keys()
    for p in shared_processes:
        if(int(VC1[p]) > int(VC2[p])):
            return False
    return True

#returns max(VC1, VC2) assuming both VCs are concurrent
def max_VC(VC1, VC2, view):
    if len(VC1) != len(VC2):
        return None
    shared_processes = VC1.keys() & VC2.keys()
    max_VC = {addy:"0" for addy in view} ## map replica sockets to their VC entry
    for p in shared_processes:
        max_VC[p] = str(max(int(VC1[p]), int(VC2[p])))
    return max_VC