####
#
#
# THIS HELPER FUNCTION FILE CONTAINS IMPLEMENTATIONS FOR CAUSAL BROADCAST OPERATIONS
# AND FUNCTIONS RELEVANT TO CAUSALITY
#
#
####

from hashlib import md5
import requests

def broadcast_down(delete, dead):  # broadcast the dead process to the specified processes
    for d in delete:
        url = "http://{}/view".format(d)
        requests.delete(url, json = {"socket-address": dead}, timeout=2)

## sharding
def key_to_shard(key, num_shard):
    val = md5(key.encode('utf8')).hexdigest()
    return f"s{int(val, 16) % num_shard}"

def isAcceptable(shard_map):  # check shard acceptance condition
    return all([False if len(m)<2 else True for _,m in shard_map.items()])

def delete_shard_mapping(ston, p):
    for s in ston:
        if p in ston[s]:
            ston[s].remove(s)
            break

# performs the shard to node and node to shard mappings
# returns TWO dictionaries, one mapping node to shard, another mapping shard to node
# returns None if the "2 nodes per shard" cannot be satisfied
def designate_shard(view, num_shard):
    if len(view) // num_shard < 2:
        return None, None, False
    sorted_list = sorted(view)
    map_IP_to_shard = dict()
    map_shard_to_IP = {f"s{_}": [] for _ in range(num_shard)}
    for i, IP in enumerate(sorted_list):
        key = f"s{i % num_shard}"
        map_IP_to_shard[IP] = key
        map_shard_to_IP[key].append(IP)
    return map_IP_to_shard, map_shard_to_IP, True

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

def check_null(VC, view):  ## makes sure you observe the subset of relevant shard view before checking causal consistency
    subset = {w:VC[w] for w in view}
    return all(i in view and j=="0" for i,j in subset.items())

def concurrent(VC1, VC2, view):
    return (not happens_before(VC1, VC2, view)) and (not happens_before(VC2, VC1, view))

def equal(VC1, VC2, view):
    for p in view:
        if VC1[p] != VC2[p]:
            return False
    return True

def happens_before(VC1, VC2, view):
    for p in view:
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