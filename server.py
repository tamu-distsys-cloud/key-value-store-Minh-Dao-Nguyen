#### COMMENTING """
# I have used Generative AI for the last task ( sharding ) of the lab to generated the logic for
#     the reponsible, replicate, and leader function. Then I debug with tests cases

import logging
import threading
from typing import Tuple, Any, List

debugging = False

class WrongShardException(Exception):
    pass

# Use this function for debugging
def debug(format, *args):
    if debugging:
        logging.info(format % args)

# Put or Append
class PutAppendArgs:
    # Add definitions here if needed
    def __init__(self, key, value, client_num, req_count, forwarded=False):
        self.key = key
        self.value = value
        self.client_num = client_num
        self.req_count = req_count
        self.forwarded = forwarded

class PutAppendReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class GetArgs:
    # Add definitions here if needed
    def __init__(self, key, client_num, req_count):
        self.key = key
        self.client_num = client_num
        self.req_count = req_count

class GetReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value
class KVServer:
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg

        self.map_data = {}
        self.req_data = {}

        #self.nshards = cfg.nservers
        #self.replication = cfg.nreplicas
        self.index = 0

    def is_responsible(self, key: str) -> bool:
        shard = sum([ord(c) for c in key]) % self.cfg.nservers
        group = [(shard + i) % self.cfg.nservers for i in range(self.cfg.nreplicas)]
        return self in [self.cfg.kvservers[i] for i in group]

    def is_leader(self, key: str) -> bool:
        shard = sum([ord(c) for c in key]) % self.cfg.nservers
        return self is self.cfg.kvservers[shard]

    # def replicate(self, method: str, args):
    #     shard = sum([ord(c) for c in args.key]) % self.nshards
    #     # for i in range(1, self.replication):
    #     #     sid = (shard + i) % self.nshards
    #     #     try:
    #     #         self.cfg.kvservers[sid].call("KVServer." + method, args)
    #     #     except:
    #     #         continue
    #     for i in range(1, self.replication):  # skip self (i = 0)
    #         sid = (shard + i) % self.nshards
    #         try:
    #             self.cfg.kvservers[sid].call("KVServer." + method, args)
    #         except:
    #             continue

    # def replicate(self, method: str, args):
    #     shard = sum([ord(c) for c in args.key]) % self.nshards
    #     for i in range(1, self.replication):  # skip the leader (i=0)
    #         sid = (shard + i) % self.nshards
    #         server = self.cfg.kvservers[sid]
    #
    #         with server.mu:
    #             if method == "Put":
    #                 server.map_data[args.key] = args.value
    #                 server.req_data[args.client_num] = (args.req_count, PutAppendReply(None))
    #             elif method == "Append":
    #                 old_val = server.map_data.get(args.key, "")
    #                 server.map_data[args.key] = old_val + args.value
    #                 server.req_data[args.client_num] = (args.req_count, PutAppendReply(old_val))

    def replicate(self, method: str, args):
        shard = sum([ord(c) for c in args.key]) % self.cfg.nservers
        for i, server in enumerate(self.cfg.kvservers):
            if server is self:
                self.index = i
        for i in range(1, self.cfg.nreplicas): #( R - 1)
            sid = (shard + i) % self.cfg.nservers
            #print("index:", self.index, "shard:", shard, "replica:", self.cfg.nreplicas, "sid:", sid)
            replica = self.cfg.kvservers[sid]

            with replica.mu:
                #print("replicate")
                if method == "Put":
                    replica.map_data[args.key] = args.value
                    replica.req_data[args.client_num] = (args.req_count, PutAppendReply(None))
                elif method == "Append":
                    old_val = replica.map_data.get(args.key, "")
                    replica.map_data[args.key] = old_val + args.value
                    replica.req_data[args.client_num] = (args.req_count, PutAppendReply(old_val))

    def Get(self, args: GetArgs):
        if not self.is_responsible(args.key):
            raise KeyError("WRONG_SHARD")

        reply = GetReply(None)
        with self.mu:
            reply.value = self.map_data.get(args.key, "")
            return reply

    def Put(self, args: PutAppendArgs):
        if not self.is_responsible(args.key):
            raise KeyError("WRONG_SHARD")

        reply = PutAppendReply(None)
        with self.mu:
            if args.client_num in self.req_data:
                last_req, last_reply = self.req_data[args.client_num]
                if last_req == args.req_count:
                    return last_reply
                elif last_req > args.req_count:
                    return reply

            # if not self.is_leader(args.key):
            #     return reply  # Drop silently — replicas shouldn't write

            self.map_data[args.key] = args.value
            self.req_data[args.client_num] = (args.req_count, reply)
            self.replicate("Put", args)

            return reply

    def Append(self, args: PutAppendArgs):
        if not self.is_responsible(args.key):
            raise KeyError("WRONG_SHARD")

        reply = PutAppendReply(None)
        with self.mu:
            if args.client_num in self.req_data:
                last_req, last_reply = self.req_data[args.client_num]
                if last_req == args.req_count:
                    return last_reply
                elif last_req > args.req_count:
                    return reply

            # if not self.is_leader(args.key):
            #     return reply  # Drop silently — replicas shouldn't write

            old_val = self.map_data.get(args.key, "")
            self.map_data[args.key] = old_val + args.value
            reply.value = old_val
            self.req_data[args.client_num] = (args.req_count, reply)
            self.replicate("Append", args)

            return reply

# class KVServer:
#     def __init__(self, cfg):
#         self.mu = threading.Lock()
#         self.cfg = cfg
#
#         # Your definitions here.
#         self.map_data = {}
#         #self.req_data = []
#         self.req_data = {}
#
#     def Get(self, args: GetArgs):
#         reply = GetReply(None)
#
#         # Your code here.
#         with self.mu:
#             if args.key in self.map_data.keys():
#                 value = self.map_data[args.key]
#                 reply.value = value
#             else:
#                 reply.value = ""
#
#             # if args.client_num in self.req_data:
#             #     # ignore how ?
#             #     return reply
#             # else:
#             #     self.req_data.append(args.client_num)
#
#             if args.client_num in self.req_data.keys():
#                 last_req, last_reply = self.req_data[args.client_num]
#                 if last_req == args.req_count:
#                     print("dup")
#                     return last_reply
#                 elif last_req > args.req_count:
#                     print("dup")
#                     return reply
#
#             return reply
#
#     def Put(self, args: PutAppendArgs):
#         reply = PutAppendReply(None)
#
#         # Your code here.
#         with self.mu:
#             #if args.key in self.map_data.keys():
#             # if args.client_num in self.req_data.keys():
#             #     if self.req_data[args.client_num][0] >= args.req_count:
#             #         return self.req_data[args.client_num][1]
#             if args.client_num in self.req_data:
#                 #print("found put")
#                 last_req, last_reply = self.req_data[args.client_num]
#                 if last_req == args.req_count:
#                     #print("dup", "-"*30)
#                     return last_reply
#                 elif last_req > args.req_count:
#                     #print("dup", "-"*30)
#                     return PutAppendReply(None)
#
#             self.map_data[args.key] = args.value
#             self.req_data[args.client_num] = (args.req_count, reply)
#             #print("req_data add:", args.req_count, args.client_num)
#             return reply
#
#     def Append(self, args: PutAppendArgs):
#         reply = PutAppendReply(None)
#
#         # Your code here.
#         with self.mu:
#             # if args.client_num in self.req_data.keys():
#             #     if self.req_data[args.client_num][0] >= args.req_count:
#             #         return self.req_data[args.client_num][1]
#             if args.client_num in self.req_data:
#                 #print("found append")
#                 last_req, last_reply = self.req_data[args.client_num]
#                 if last_req == args.req_count:
#                     #print("dup", "-"*30)
#                     return last_reply
#                 elif last_req > args.req_count:
#                     #print("dup", "-"*30)
#                     return PutAppendReply(None)
#
#             # if args.key in self.map_data.keys():
#             #     old_val = self.map_data[args.key]
#             #     self.map_data[args.key] = old_val + args.value
#             #     reply.value = old_val
#             # else:
#             #     self.map_data[args.key] = args.value
#             #     reply.value = ""
#
#             old_val = self.map_data.get(args.key, "")
#             self.map_data[args.key] = old_val + args.value
#             reply = PutAppendReply(old_val)
#
#             self.req_data[args.client_num] = (args.req_count, reply)
#             return reply
