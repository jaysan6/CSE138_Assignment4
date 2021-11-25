from flask import Flask, request, jsonify
import os, requests
from helper_func import list_less1, list_less2, max_VC, happens_before, concurrent, equal
import time
import threading

keyvalue_app = Flask(__name__)

VIEWERS = os.environ.get("VIEW")
CURRENT_REPLICA = os.environ.get("SOCKET_ADDRESS")

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

    content = request.get_json()
    response = dict()

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
        if VC_sender is not None:  # check if CLIENT message satisfies causal dependencies
            if not concurrent(VC_local, VC_sender) and not equal(VC_sender, VC_local):
                response = {"error": "Causal dependencies not satisfied"}
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
            response = {"error": "Key does not exist", "view": queue, "store":store, "broadcast": broadcast, "VC_local": content}
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
def view():
    if request.method == 'PUT':
        if not request.is_json:  # if the request is a JSON object
            return jsonify({"error":"not a valid JSON object"}), 400

        new_socket = request.get_json()
        replica_IP = new_socket["socket-address"]

        if replica_IP not in view:
            view.append(replica_IP)
            response = {"result": "added"}
            return jsonify(response), 201
        else:
            response = {"result": "already present"}
            return jsonify(response), 200
    elif request.method == 'GET':
        response = {"view": view}
        return jsonify(response), 200
    elif request.method == 'DELETE':
        if not request.is_json:  # if the request is a JSON object
            return jsonify({"error":"not a valid JSON object"}), 400

        new_socket = request.get_json()
        replica_IP = new_socket["socket-address"]

        if replica_IP in view:
            view.remove(replica_IP)
            response = {"result": "deleted"}
            return jsonify(response), 200
        else:
            response = {"error": "View has no such replica"}
            return jsonify(response), 404

@keyvalue_app.route("/share", methods = ['GET'])
def update_new_replica():
    return jsonify([store, VC_local]), 200 ## return the kvs

@keyvalue_app.route("/VC", methods = ['GET'])
def get_VC():
    return jsonify(VC_local), 200

##shard operations

#retrieve a list of all hard identifiers unconditionally
# – Response code is 200 (Ok).
# – Response body is JSON {"shard-ids": [<ID>, <ID>, ...]}.
# – Shard identifiers can be strings or numbers and their order is not important. For example, if you
# have shards "s1" and "s2" the response body could be {"shard-ids": ["s2", "s1"]}.
@keyvalue_app.route("/shard/ids", methods = ['PUT', 'GET', 'DELETE'])
def shard_client():
    if request.method == 'GET':
        pass


#– Response code is 200 (Ok).
# – Response body is JSON {"node-shard-id": <ID>}.
# – Here is an example, if the node at <IP:PORT> belonged to the shard "apples":

@keyvalue_app.route("/shard/node-shard-id", methods = ['PUT', 'GET', 'DELETE'])

def shard_node_client():
    if request.method == 'GET':
        pass

#GET
#
#
# Look up the members of the indicated shard.
# •If the shard <ID> exists, then return the list of nodes from the view who are in the shard.
# – Response code is 200 (Ok).
# – Response body is JSON {"shard-members": ["<IP:PORT>", "<IP:PORT>", ...]}.
# •If the shard <ID> does not exist, then respond with a 404 error.


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


@keyvalue_app.route("/shard/members/<ID>", methods = ['PUT', 'GET', 'DELETE'])

def shard_members_client():
    if request.method == 'GET':
        pass
    elif request.method == 'PUT':
        pass

#GET
#
#
# Look up the number of key-value pairs stored by the indicated shard. To implement this endpoint, it may be
# necessary to delegate the request to a node in the indicated shard.
# •If the shard <ID> exists, then return the number of key-value pairs are stored in the shard.
# – Response code is 200 (Ok).
# – Response body is JSON {"shard-key-count": <INTEGER>}.
# •If the shard <ID> does not exist, then respond with a 404 error

@keyvalue_app.route("/shard/key-count/<ID>", methods = ['PUT', 'GET', 'DELETE'])
def shard_keycount_client():
    if request.method == 'GET':
        pass


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
@keyvalue_app.route("/shard/reshard", methods = ['PUT', 'GET', 'DELETE'])

def shard_reshard_client():
    if request.method == 'PUT':
        pass



if __name__ == '__main__':
    store = dict()
    view = VIEWERS.split(',')  ## parse VIEW string (given as environment variable)
    VC_local = {addy:"0" for addy in view} ## map replica sockets to their VC entry
    queue = list()  # ready queue used in causal broadcast if dependencies not satisfied

    update_kvs = None
    for v in view:    ## check on the replicas in the view and see if CURRENT needs to be updated
        if v != CURRENT_REPLICA:
            try:  ## add current to the other replicas' view
                url = "http://{}/view".format(v)
                broadcast_json = {"socket-address": CURRENT_REPLICA}
                response = requests.put(url, json = broadcast_json, timeout = 2)
                if response.status_code == 201 or response.status_code == 200:  # if it can be put, ask for replica kvs (for consistency)
                    update_kvs = True
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                pass

    if update_kvs:  ## if this is a replica that starts up in an already-existing kvs, set own kvs to another replica's kvs
        for v in view:
            if v != CURRENT_REPLICA:
                try:
                    url = "http://{}/share".format(v)
                    response = requests.get(url, timeout = 2)
                    store, VC_local = response.json()
                    break   ## Since they are all replicas, if we can successfully update 1, then dont need to continue iterating
                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                    pass  ## keep trying other replicas if they fail

    keyvalue_app.run('0.0.0.0', port = 8090, debug = True)
