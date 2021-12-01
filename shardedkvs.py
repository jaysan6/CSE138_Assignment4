from flask import Flask, request, jsonify
import os, requests, time, threading
from helper_func import designate_shard, key_to_shard, list_less1, list_less2, max_VC, concurrent, equal, check_null

keyvalue_app = Flask(__name__)

VIEWERS = os.environ.get("VIEW")
CURRENT_REPLICA = os.environ.get("SOCKET_ADDRESS")
SH = os.environ.get("SHARD_COUNT")
SHARD_COUNT = int(SH) if SH is not None else SH


@keyvalue_app.route("/kvs", methods = ['PUT', 'GET', 'DELETE']) # if no key passed
def empty_key1():
    return jsonify({"error":"no key pased"}), 404

@keyvalue_app.route("/kvs/", methods = ['PUT', 'GET', 'DELETE', 'POST']) # if no key passed
def empty_key2():
    return jsonify({"error":"no key pased"}), 404

#if key not empty
@keyvalue_app.route("/kvs/<key>", methods = ['PUT', 'GET', 'DELETE'])
def keyvalue_store(key):
    if not key.strip():  # if a bunch of spaces are passed as a key
        return jsonify({"error":"no key pased"}), 404

    if not inShard:
        return jsonify({"error":"not in a shard"}), 400

    content = request.get_json()
    response = dict()
    
    forward = "forward" not in content.keys()
    
    if forward:
        designated_shard = key_to_shard(key, len(shard_to_node))  ## forward the request if key not supposed to be put in shard
        if designated_shard != node_to_shard[CURRENT_REPLICA]:
            forward_node = shard_to_node.get(designated_shard)
            for n in forward_node:
                url = f"http://{n}/kvs/{key}"
                if request.method == 'PUT':
                    response = requests.put(url, json=content)
                elif request.method == 'DELETE':
                    response = requests.delete(url, json=content)
                else:
                    response = requests.get(url, json=content)
                return response.text, response.status_code


        for q in queue:
            VC, k, val, IP, op, w = q  # extract necessary data from prior request
            causally_consistent = list_less1(VC, VC_local, IP) and list_less2(VC, VC_local, IP)
            if causally_consistent:  # process the request if it can be processed
                VC_new = max_VC(VC_local, VC, view)
                if VC_new is not None:
                    for keys in VC_new.keys():
                        VC_local[keys] = VC_new[keys]
                if op == "put": # do the actual operation so it catches up
                    store[k] = val
                if op == "del" and k in store:
                    del store[k]
                queue.remove(q)

    broadcast = "broadcast" in content.keys()  # identifier for if the message is a broadcast
    VC_sender = content["causal-metadata"]

    ## if broadcast is true, then broadcast message, otherwise client message
    if not broadcast:
        if VC_sender is not None and not check_null(VC_sender, view):  # check if CLIENT message satisfies causal dependencies
            if not concurrent(VC_local, VC_sender, view) and not equal(VC_sender, VC_local, view):
                response = {"error": "Causal dependencies not satisfied", "view": view}
                return jsonify(response), 503

    def retry_broadcast(**kwargs):  ## multithreading (background retry broadcasts)
        again = kwargs.get('retry', CURRENT_REPLICA)
        op = kwargs.get('type', CURRENT_REPLICA)
        if op == "put":
            broadcast_json = {"value": content["value"], "causal-metadata": VC_local, "broadcast": CURRENT_REPLICA}
        else:
            broadcast_json = {"causal-metadata": VC_local, "broadcast": CURRENT_REPLICA}
        url = "http://" + again + "/kvs/" + key

        while True:  # HTTP LONG POLLING
            try:
                if op == "put":
                    req = requests.put(url, json = broadcast_json, timeout = 5)
                else:
                    req = requests.delete(url, json = broadcast_json, timeout = 5)

                if req.status_code == 503:
                    time.sleep(1)
                else:
                    break
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                delete = set(view) - set([again])
                for v in delete:
                    url = "http://{}/view".format(v)
                    requests.delete(url, json = {"socket-address": again}, timeout=2) # we actually find that the replica died
                break

    def broad(op):
        deaths = list()  # lists of processes that this current process identifies as dead
        retry = list()   # list of processes that return causal dependencies not satisfied (503)
        VC_local[CURRENT_REPLICA] = str(int(VC_local[CURRENT_REPLICA]) + 1)

        for v in view: ## broadcast the view
            if v != CURRENT_REPLICA:
                url = "http://" + v + "/kvs/" + key
                try:
                    if op == 'put':
                        broadcast_json = {"value": content["value"], "causal-metadata": VC_local, "broadcast": CURRENT_REPLICA}
                        response = requests.put(url, json = broadcast_json, timeout = 5)
                    else:
                        broadcast_json = {"causal-metadata": VC_local, "broadcast": CURRENT_REPLICA}
                        response = requests.delete(url, json = broadcast_json, timeout = 5)
                    if response.status_code == 503:  # causal dependencies not satisfied
                        retry.append(v)
                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                    deaths.append(v)

        delete = set(view) - set(deaths) # replicas we want to broadcast the death of a replica to (all replicas still alive)
        for dead in deaths:  # broadcast dead process to other replicas (so they update their view)
            for d in delete:
                url = "http://{}/view".format(d)
                requests.delete(url, json = {"socket-address": dead}, timeout=2)

        for r in retry:  # keep trying to send the put for the retry processes
            thread = threading.Thread(target=retry_broadcast, kwargs={'retry': r, "type": str(op)})
            thread.start()

    def check_causal_consistency(op):  ## op is the type of write (put or del)
        sendIP = str(request.remote_addr) + ":8090"
        causally_consistent = list_less1(VC_sender, VC_local, sendIP) and list_less2(VC_sender, VC_local, sendIP)
        if causally_consistent:
            VC_new = max_VC(VC_local, VC_sender, view)
            if VC_new is not None:
                for keys in VC_new.keys():
                    VC_local[keys] = VC_new[keys]
        else:
            if op == 'put':
                metadata = [VC_sender, key, content["value"], sendIP, "put", VC_local]
            else:
                metadata = [VC_sender, key, None, sendIP, "del"]
            queue.append(metadata)  # append to queue for later processing
        return causally_consistent

    if request.method == 'PUT':
        if not request.is_json:  # if the request is a JSON object
            return jsonify({"error":"not a valid JSON object"}), 400

        if len(key) > 50:
            response = {"error": "Key is too long"}
            return jsonify(response), 400

        if "value" not in content.keys():
            response = {"error": "PUT request does not specify a value"}
            return jsonify(response), 400
        
        cc = True
        if forward:
            if not broadcast:
                broad(op='put')
            else:
                cc = check_causal_consistency(op='put')
                
        if not key in store:
            if cc:
                store[key] = content["value"]
            response = {"result": "created", "causal-metadata": VC_local}
            return jsonify(response), 201
        else:
            if cc:
                store[key] = content["value"]
            response = {"result": "replaced", "causal-metadata": VC_local}
            return jsonify(response), 200
    elif request.method == 'GET':
        if key in store:
            response = {"result": "found", "value":  store[key], "causal-metadata": VC_local}
            return jsonify(response), 200
        else:
            response = {"error": "Key does not exist"}
            return jsonify(response), 404
    elif request.method == 'DELETE':
        cc = True
        if not broadcast:
            broad(op='del')
        else:
            cc = check_causal_consistency(op='del')

        if key in store:
            if cc:
                del store[key]
            response = {"result": "deleted", "causal-metadata": VC_local}
            return jsonify(response), 200
        else:
            response = {"error": "Key does not exist"}
            return jsonify(response), 404

@keyvalue_app.route("/view", methods = ['PUT', 'GET', 'DELETE'])
def process_view():
    if request.method == 'PUT':
        if not request.is_json:  # if the request is a JSON object
            return jsonify({"error":"not a valid JSON object"}), 400

        new_socket = request.get_json()
        replica_IP = new_socket["socket-address"]

        if replica_IP not in total_view:
            total_view.append(replica_IP)
            VC_local[replica_IP] = "0"
            response = {"result": "added"}
            return jsonify(response), 201
        else:
            response = {"result": "already present"}
            return jsonify(response), 200
    elif request.method == 'GET':
        response = {"view": total_view}
        return jsonify(response), 200
    elif request.method == 'DELETE':
        if not request.is_json:  # if the request is a JSON object
            return jsonify({"error":"not a valid JSON object"}), 400

        new_socket = request.get_json()
        replica_IP = new_socket["socket-address"]

        if replica_IP in total_view:
            total_view.remove(replica_IP)
            response = {"result": "deleted"}
            return jsonify(response), 200
        else:
            response = {"error": "View has no such replica"}
            return jsonify(response), 404

@keyvalue_app.route("/share", methods = ['GET', 'PUT'])
def update_new_replica():
    if request.method == "GET":
        return jsonify({"store": store, "VC": VC_local}), 200
    else:
        update = request.get_json()  # this replaces the local kvs, vc with the json specified data
        store.clear()
        if "VC" in update.keys():
            VC_local.clear()
            VC_local.update(update.get("VC"))
        store.update(update.get("kvs"))
        return jsonify({"result": "updated"}), 200

@keyvalue_app.route("/VC", methods = ['GET'])
def get_VC():
    return jsonify(VC_local), 200

@keyvalue_app.route("/access-store", methods = ['GET'])
def get_store():
    return jsonify(store), 200

##shard operations

@keyvalue_app.route("/shardcount", methods = ['GET'])
def sc():
    response = {"val": len(shard_to_node.keys())}
    return jsonify(response), 200


#retrieve a list of all shard identifiers unconditionally
# – Response code is 200 (Ok).
# – Response body is JSON {"shard-ids": [<ID>, <ID>, ...]}.
# – Shard identifiers can be strings or numbers and their order is not important. For example, if you
# have shards "s1" and "s2" the response body could be {"shard-ids": ["s2", "s1"]}.
@keyvalue_app.route("/shard/ids", methods = ['GET'])
def shard_client():
    return jsonify({"shard-ids": list(shard_to_node.keys())}), 200

# Retrieve the shard identifier of the responding node, unconditionally.
# – Response code is 200 (Ok).
# – Response body is JSON {"node-shard-id": <ID>}.
@keyvalue_app.route("/shard/node-shard-id", methods = ['GET'])
def shard_node_client():
    return jsonify({"node-shard-id": node_to_shard.get(CURRENT_REPLICA)}), 200


#GET
#
#
# Look up the members of the indicated shard.
# •If the shard <ID> exists, then return the list of nodes from the view who are in the shard.
# – Response code is 200 (Ok).
# – Response body is JSON {"shard-members": ["<IP:PORT>", "<IP:PORT>", ...]}.
# •If the shard <ID> does not exist, then respond with a 404 error.
@keyvalue_app.route("/shard/members/<ID>", methods = ['GET'])
def shard_members_client(ID):
    if ID not in shard_to_node:
        return jsonify({"error": "no such shard ID exists"}), 404
    else:
        return jsonify({"shard-members": shard_to_node.get(ID)}), 200

#GET
#
#
# Look up the number of key-value pairs stored by the indicated shard. To implement this endpoint, it may be
# necessary to delegate the request to a node in the indicated shard.
# •If the shard <ID> exists, then return the number of key-value pairs are stored in the shard.
# – Response code is 200 (Ok).
# – Response body is JSON {"shard-key-count": <INTEGER>}.
# •If the shard <ID> does not exist, then respond with a 404 error
@keyvalue_app.route("/shard/key-count/<ID>", methods = ['GET'])
def shard_keycount_client(ID):
    if ID not in shard_to_node:
        return jsonify({"error": "no such shard ID exists"}), 404
    else:
        processes = shard_to_node.get(ID)
        if CURRENT_REPLICA in processes:
            count = len(store)
        else:  ## ask a process that actually is in the specified shard for the key count
            for node in processes:
                url = f"http://{node}/shard/key-count/{ID}"
                try:
                    resp = requests.get(url)
                    data = resp.json()
                    count = data.get("shard-key-count")
                    break
                except requests.exceptions.ConnectionError:
                    pass
            x = False
        return jsonify({"shard-key-count": count}), 200

#PUT
#
#
# Assign the node <IP:PORT> to the shard <ID>. The responding node may be different from the node indicated
# by <IP:PORT>.
# •If the shard <ID> exists and the node <IP:PORT> is in the view, assign that node to be a member of that
# shard.
# – Response code is 200 (Ok).
# – Response body is JSON {"result": "node added to shard"}
# If either the shard <ID> or the node <IP:PORT> doesn’t exist, respond with a 404 error.
# For other error conditions, respond with a 400 error. This isn’t tested

@keyvalue_app.route("/shard/add-member/<ID>", methods = ['PUT'])
def add_member(ID):
    node = request.get_json()["socket-address"]

    if "broadcast" in request.get_json().keys():  ## add member to process' local shard mapping if a broadcast msg
        node_to_shard[node] = ID
        shard_to_node[ID].append(node)
        return jsonify({"result": "updated mapping"}), 201

    if ID in shard_to_node and node in total_view:
        node_to_shard[node] = ID
        broadcast_stuff = set(total_view) - {CURRENT_REPLICA}
        for n in broadcast_stuff: ## broadcast the mapping update to all other processes
            url = f"http://{n}/shard/add-member/{ID}"
            json = {"socket-address":node, "broadcast":"0"}
            requests.put(url, json=json)

        url = f"http://{node}/share"
        data = {"VC": VC_local, "kvs": store}
        if CURRENT_REPLICA not in shard_to_node[ID]:  ## replicate the correct process' kvs to the newly added node
            for n in shard_to_node[ID]:
                u = f"http://{n}/share"
                try:
                    resp = requests.get(u)
                    data["VC"]= resp.json()["VC"]
                    data["kvs"] = resp.json()["store"]
                    break
                except requests.exceptions.ConnectionError:
                    pass
        requests.put(url, json=data)  ## replicate (see implementation in the endpoint)
        shard_to_node[ID].append(node)
        return jsonify({"result": "node added to shard", "data": data}), 200
    else:
        return jsonify({"error": "node or shard does not exist"}), 404


#PUT REQUEST
# Trigger a reshard into <INTEGER> shards, maintaining fault-tolerance invariant that each shard contains at
# least two nodes. There’s more information in the section Resharding the key-value store, below.
# •If the fault-tolerance invariant would be violated by resharding to the new shard count, then return an
# error.
# 4
# – Response code is 400 (Bad Request).
# – Response body is JSON {"error": "Not enough nodes to provide fault tolerance with
# requested shard count"}
# •If the fault-tolerance invariant would not be violated, then reshard the store.
# – Response code is 200 (Ok).
# – Response body is JSON {"result": "resharded"}
@keyvalue_app.route("/shard/reshard", methods = ['PUT'])
def shard_reshard_client():
    req = request.get_json()
    new_shard = int(req["shard-count"])
    x,y,z = designate_shard(total_view, new_shard)
    if not z:
        return jsonify({"error": "Not enough nodes to provide fault tolerance with requested shard count"}), 400
    else:
        for i in total_view:
            url = f"http://{i}/shard/perform-reshard"
            data = {"ntos": x, "ston": y}
            requests.put(url, json=data)
        return jsonify({"result": "resharded"}), 200

@keyvalue_app.route("/shard/perform-reshard", methods = ['PUT'])
def reshard():
    mapping = request.get_json()
    ntos = mapping.get("ntos")
    ston = mapping.get("ston")
    node_to_shard.clear()
    shard_to_node.clear()
    node_to_shard.update(ntos)
    shard_to_node.update(ston)
    
    view.clear()
    view.extend(shard_to_node.get(node_to_shard.get(CURRENT_REPLICA)))
    
    shardcount = len(shard_to_node)
    
    keys = list()
    for key,val in store.items():
            shard = key_to_shard(key, shardcount)
            if shard != node_to_shard[CURRENT_REPLICA]:
                keys.append((shard, key, val))

    [store.pop(k) for _,k,unused in keys]
    
    for s, k, v in keys:
        for n in shard_to_node[s]:
            try:
                url = "http://{}/kvs/{}".format(n, k)
                requests.put(url, json={'value':v, 'causal-metadata':None, "forward": "Yes"})
            except requests.exceptions.ConnectionError:
                pass
    return jsonify({"data": shardcount}), 200

if __name__ == '__main__':
    store = dict()
    total_view = VIEWERS.split(',')  ## parse VIEW string (given as environment variable)
    VC_local = {addy:"0" for addy in total_view} ## map replica sockets to their VC entry
    queue = list()  # ready queue used in causal broadcast if dependencies not satisfied
    
    inShard = SHARD_COUNT is not None  # if a shard is not specified a count, cannot be used until added
    view = list()

    ### SHARD ASSIGNMENTS ON STARTUPss
    if inShard:
        node_to_shard, shard_to_node, condition = designate_shard(total_view, SHARD_COUNT)
        shard = node_to_shard[CURRENT_REPLICA]
        view = shard_to_node.get(shard) # this is the new local view
        restart_view = set(view) - {CURRENT_REPLICA}
    else:
        without_this_shard = list(set(total_view) - {CURRENT_REPLICA})
        for node in without_this_shard:
            url = f"http://{node}/shardcount"
            resp = requests.get(url)
            sc = resp.json()["val"]  ## extract the shard count from another pcoess in the view
            break
        node_to_shard, shard_to_node, condition = designate_shard(without_this_shard, sc)  ## figure out the mapping using other process' data
        shard = {}
        view = [] # this is the new local view
        restart_view = {}
    
    overall_restart_view = set(total_view) - {CURRENT_REPLICA}
    update_kvs = None
    for v in overall_restart_view:    ## check on the replicas in the view and see if CURRENT needs to be updated
        try:  ## add current to the other replicas' view
            url = "http://{}/view".format(v)
            broadcast_json = {"socket-address": CURRENT_REPLICA}
            response = requests.put(url, json = broadcast_json, timeout = 2)
            if response.status_code == 201 or response.status_code == 200:  # if it can be put, ask for replica kvs (for consistency)
                update_kvs = True
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            pass
    
    if inShard and update_kvs:  ## if this is a replica that starts up in an already-existing kvs, set own kvs to another replica's kvs
        for v in restart_view:
            try:
                url = "http://{}/share".format(v)
                response = requests.get(url, timeout = 2)
                store, VC_local = response.json()["store"], response.json()["VC"]
                break   ## Since they are all replicas, if we can successfully update 1, then dont need to continue iterating
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                pass  ## keep trying other replicas if they fail

    keyvalue_app.run('0.0.0.0', port = 8090, debug = True)