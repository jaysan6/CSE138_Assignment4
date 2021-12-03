# CSE138 Assignment 4

## TEAM: ARKA PAL & SANJAY SHRIKANTH

## Acknowledgements:

TA Patrick Redmond

- Provided insight into the assignment and helped with clarifying any doubts and confusions regarding the technicalities behind sharding

- Gave helpful suggestions for implementing the allocation of replicas to their respective shards in a way that is evenly distributed in section

- Recommended using _deterministic_ random hashing to divide the keys into shards so that each process can distribute the keys in the same way. Introduced us to the ```hashlib``` library and potential functions like ```sha#``` and how to process their respective outputs

_Zulip_ Discussion Forum

- Clarified many of our doubts regarding the notion of causal metadata among shards and how our distributed system should narrow its scope to the processes in its own shard

*We did not collaborate with anyone else on this assignment*

## Citations

- https://docs.python.org/3/library/hashlib.html
  
  - We used this documentation as a reference to implement our key-to-shard hash function. Since the hashlib function returns ```hash``` objects, the code examples in this documentation were useful in helping us understand how to process those objects into strings, and ultimately numbers

- [Quickstart — Requests 2.26.0 documentation](https://docs.python-requests.org/en/latest/user/quickstart/)
  
  - This documentation was extensively used in implementing communication between replica, as the `requests` module was not only a mechanism for sending data, but also a way to debug with the tester file using `response.text`and helpful router ```get``` functions

- Shard Designation and Key hashing Pseudocode from TA Patrick Redmond
  
  - Used his provided pseudocode from section as a reference for our implementation in the sharding process. Helped us understand the steps in the __“Hash key mod N”__ approach in practice

## Team Contributions

Sanjay

- Implemented the ```VC``` and ```share``` functions which both serve as mechanisms in settting or getting the vector clocks and key-values from different processes respectively

- Implemented the routes and helped with designing the methodology behind ```\reshard``` and ```\add-member```. Also coded the ```\shard_ids``` routing function 

- Designed the algorithms behind deisgnating the local and total shard views on start-up, accounting for when a process is given a shard and when it is not

- Incorporated fault tolerance and down detection in the sharded key-value store in both the local and total view perspectives

- Devised the helper functions ```designate_shard``` that performs the node to shard and shard to node mappings

Arka

- Coded the ```\key-count```, ```\shard-count```, ```\member```, ```\ids``` router functions

- Implemented the mechanism behind checking if a reshard was possible or not

- Provided a starting point in the implementation of the resharding process

- Partook in debugging and testing out extraneous edge cases to make sure the program functioned properly by modifying values in the provided test scripts

- Redid the helper functions in ```helper_func.py``` in a manner that minimizes looping (which is slow in python) and optimized them for efficient and quick computation

## Mechanism Descriptions

##### Implementing Shard Mappings and Local Views

We implement a bidirectional mapping mechanism that allows a process to see the corresponding shard given a process, and vice versa. This creates a construct that is both used in the many routing functions we implement in our system, as they serve as a form of identification for the process we are currently referencing. The mapping allows for a process to have a “total view” and a “local view”. The total view is all the processes currently running and the “local view” is the replicas that are part of a process’ shard.

##### Tracking Causal Dependencies

We implemented the causal broadcast algorithm from class using vector clocks as our causal metadata. In code, the vector clocks are python ```dict()``` which map the replica's IP address to the respective vector clock value. We only implement the causal broadcast algorithm for put and delete requests, as get requests are idempotent and do not need to be broadcasted to other replicas. We also do not implement the algorithm for client messages, as we are in the asynchronous model and therefore do not need to be causally consistent. The client partakes in causal-metadata propagation and allows our key-value store to detect causal inconsistencies at a certain replica before processing it. When implementing the algorithm, we first increment the sender's entry of the vector clock and check if 2 processes are concurrent. If they are not concurrent and not equal, we return a 503. We use vector clock equality in making sure replicas are causally consistent, as if the broadcast is working properly, each replica should perform the same sequence of operations in causal order. Otherwise, we set the current processes's vector clock to the max of the sender and current. Finally, if the vector clocks of the 2 processes are causally consistent, we deliver the message, else we add the process's metadata to a queue. We iterate through the queue and make sure processes are delivered in causal order by repeating the same steps for each entry in the queue. Akin to the queue in the causal broadcast algorithm, when a process cannot process a broadcast message, our key-value store adds the operation to a queue and processes it later when causal dependencies are satisfied. Lastly, If a broadcast message throws a causal dependency error, or a 503, then we implemented _HTTP Long Polling_ to constantly retry the operations until causal order is satisfied.

With regards to sharding, causal dependencies are tracked in the same way, but only within the scope of a process’ shard.  So, when a process gets a ```put``` or ```delete``` request, the operation is broadcasted only to the processes in the respective shard, only incrementing the causal metadata in the local view scope.

##### Detecting if a replica goes down

We use a `try/except` mechanism to detect downed replicas. We check the `requests.exceptions.ConnectionError` and `requests.exceptions and requests.exceptions.Timeout`, which indicates, that a processes is not available or a process is taking a long time to recieve a message. We then take the intersection of these processes along with the processes in the local view, gives us all the dead processes, and delete such processes. We ensure to update _both_ the total view, the local view, and the shard mappings to ensure consistency in the view and broadcast operations.

##### How we shard keys across nodes

To shard keys across nodes in the system, we implemented the “Hash key $\mod N$” approach described in class. To do this, we created a helper function ```key_to_shard``` that takes in the key string and the number of replicas needed to be sharded (```shard_count``` or N). To ensure that the distribution is deterministically random, or randomized but reproducible by different processes, we used the ```python hashlib``` function ```md5``` to encode the key into a hash object. With this hash object, we convert it to a hex byte-string using ```hexdigest()```. Finally, we take the modulo of the integer representation by the number of shards in the system to designate the key to its respective shard. This mechanism proved to be successful as the hashing approach distributes the keys relatively evenly across shards. Through the hash function's property of being deterministically random, processing the output into a shard identifier made it so that each key could be reproducibly hashed to a random shard on creation.