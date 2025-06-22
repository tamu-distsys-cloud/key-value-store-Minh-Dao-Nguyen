import random
import threading
from typing import Any, List
import time
from labrpc.labrpc import ClientEnd
from server import GetArgs, GetReply, PutAppendArgs, PutAppendReply, WrongShardException

def nrand() -> int:
    return random.getrandbits(62)
MAX_TRIES = 10000
class Clerk:
    def __init__(self, servers: List[ClientEnd], cfg):
        self.servers = servers
        self.cfg = cfg

        # Your definitions here.
        self.num = nrand()
        self.req_count = 0
        #print(len(servers))

    def get(self, key: str) -> str:
        #self.req_count += 1
        #shard = int(key) % self.cfg.nservers
        shard = sum([ord(c) for c in key]) % self.cfg.nservers
        servers = [(shard + i) % self.cfg.nservers for i in range(self.cfg.nreplicas)]
        args = GetArgs(key, self.num, self.req_count)

        for i in range(MAX_TRIES):
            try:
                for idx in servers:
                    try:
                        reply = self.servers[idx].call("KVServer.Get", args)
                        return reply.value
                    except:
                        continue
            except:
                continue
            time.sleep(0.05) # for some reason this is needed or it return wrong value

    def put_append(self, key: str, value: str, op: str) -> str:
        self.req_count += 1
        #shard = int(key) % self.cfg.nservers
        shard = sum([ord(c) for c in key]) % self.cfg.nservers
        servers = [(shard + i) % self.cfg.nservers for i in range(self.cfg.nreplicas)]
        args = PutAppendArgs(key, value, self.num, self.req_count)

        for i in range(MAX_TRIES):
            try:
                for idx in servers:
                    reply = self.servers[idx].call("KVServer." + op, args)
                    return reply.value
            except:
                continue
            time.sleep(0.05)


    # Fetch the current value for a key.
    # Returns "" if the key does not exist.
    # Keeps trying forever in the face of all other errors.
    #
    # You can send an RPC with code like this:
    # reply = self.server[i].call("KVServer.Get", args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    # def get(self, key: str) -> str:
    #     # You will have to modify this function.
    #     #get_args = GetArgs(key, self.num, self.req_count)
    #     #self.req_count += 1
    #     #reply = self.server[i].call("KVServer.Get", get_args)
    #     shard = int(key) % self.cfg.nservers
    #     self.req_count += 1
    #     tries = 0
    #     while True:
    #         #for i in self.servers:
    #         for i in range(self.replication):
    #             sid = (shard + i) % self.cfg.nservers
    #             try:
    #                 get_args = GetArgs(key, self.num, self.req_count)
    #                 #reply = self.servers[0].call("KVServer.Get", get_args)
    #                 #reply = i.call("KVServer.Get", get_args)
    #                 reply = self.servers[sid].call("KVServer.Get", get_args)
    #                 #print("called get key", key)
    #                 return reply.value
    #             except:
    #                 continue
    #         time.sleep(0.05)
    #         tries += 1
    #         if tries == 10000:
    #             break
    #         # for i in self.servers:
    #         #     try:
    #         #         get_args = GetArgs(key, self.num, self.req_count)
    #         #         self.req_count += 1
    #         #         reply = i.call("KVServer.Get", get_args)
    #         #         print("called get key", key)
    #         #         return reply.value
    #         #     except:
    #         #         continue
    #
    # # Shared by Put and Append.
    # #
    # # You can send an RPC with code like this:
    # # reply = self.servers[i].call("KVServer."+op, args)
    # # assuming that you are connecting to the i-th server.
    # #
    # # The types of args and reply (including whether they are pointers)
    # # must match the declared types of the RPC handler function's
    # # arguments in server.py.
    # def put_append(self, key: str, value: str, op: str) -> str:
    #     # You will have to modify this function.
    #     # put_append_arg = PutAppendArgs(key, value, self.num, self.req_count)
    #     # self.req_count += 1
    #     #reply = self.servers[i].call("KVServer." + op, put_append_arg)
    #     # for i in self.servers:
    #     #     try:
    #     #         put_append_arg = PutAppendArgs(key, value, self.num, self.req_count)
    #     #         self.req_count += 1
    #     #         reply = i.call("KVServer." + op, put_append_arg)
    #     #         check = True if value else False
    #     #         print("call", op, key, value, check)
    #     #         #if reply.value:
    #     #         return reply.value
    #     #     except:
    #     #         continue
    #     # return ""
    #
    #     #int(key) % nshards
    #     shard = int(key) % self.cfg.nservers
    #     self.req_count += 1
    #     tries = 0
    #     while True:
    #         #for i in self.servers:
    #         for i in range(self.replication):
    #             sid = (shard + i) % self.cfg.nservers
    #             try:
    #                 put_append_arg = PutAppendArgs(key, value, self.num, self.req_count)
    #                 #reply = self.servers[0].call("KVServer." + op, put_append_arg)
    #                 #reply = i.call("KVServer." + op, put_append_arg)
    #                 reply = self.servers[sid].call("KVServer." + op, put_append_arg)
    #                 #check = True if value else False
    #                 #print("call", op, key, value, check)
    #                 # if reply.value:
    #                 return reply.value
    #             except:
    #                 continue
    #         time.sleep(0.05)
    #         tries += 1
    #         if tries == 10000:
    #             break
    #
    #     # if op == "Put":
    #     #     reply = self.servers[i].call("KVServer.Put", put_append_arg)
    #     # elif op == "Append":
    #     #     reply = self.servers[i].call("KVServer.Append", put_append_arg)
    #     return ""

    def put(self, key: str, value: str):
        self.put_append(key, value, "Put")

    # Append value to key's value and return that value
    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")
