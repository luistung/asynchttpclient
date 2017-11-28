#!/bin/env python3
import sys
import aiohttp
import asyncio
import async_timeout
import argparse
import heapq
from typing import NamedTuple, Generator, Callable, Any

HeapItem = NamedTuple("HeapItem", [('id', int), ('obj', object)])


class ResultHeapq:
    def __init__(self):
        self.next = 0
        self.heap = []

    def pushpop(self, item):
        self.push(item)
        return self.pop()

    def push(self, item):
        tmpItem = (item.id, item.obj)
        heapq.heappush(self.heap, tmpItem)

    def pop(self):
        ret = []
        while self.heap and self.heap[0][0] == self.next:
            tmpItem = heapq.heappop(self.heap)
            ret.append(HeapItem(tmpItem[0], tmpItem[1]))
            self.next += 1
        return ret


class Null:
    pass


class RequestObject:
    def __init__(self, url=None, data=None, ext={}):
        self.url = url
        self.data = data
        self.ext = ext


class AsyncHttpClient():
    def __init__(self, method, repHandler: Callable[[RequestObject, str, bytes], Any], url=None, bodyHandler=lambda a: a, thread=1) ->None:
        self.method = method
        self.repHandler = repHandler
        self.url = url
        self.bodyHandler = bodyHandler
        self.thread = thread
        self.loop = asyncio.get_event_loop()
        self.sem = asyncio.Semaphore(value=thread)
        self.finish = False
        self.id = 0

    async def request(self, session, id_, reqObj):
        with async_timeout.timeout(10):
            if reqObj.url is not None:
                url = reqObj.url
            elif self.url is not None:
                url = self.url
            else:
                raise Except('url not found')
            try:
                async with session.get(url) as response:
                    data = await response.read()
                    return id_, (reqObj, response, data)
            except Exception as e:
                print(e)
                print("error in request", file=sys.stderr)
                return id_, (reqObj, None, None)

    def producer(self, session, reqObjIter, num):
        futureList = []
        for i in range(num):
            reqObj = next(reqObjIter, Null)
            if reqObj is Null:
                self.finish = True
                break
            future = asyncio.ensure_future(
                self.request(session, self.id, reqObj))
            futureList.append(future)
            self.id += 1
        return futureList

    async def doRun(self, reqObjIter):
        with aiohttp.ClientSession(loop=self.loop) as session:
            resultHeapq = ResultHeapq()
            futureList = self.producer(session, reqObjIter, self.thread)
            while True:
                done, pending = await asyncio.wait(futureList, return_when=asyncio.FIRST_COMPLETED)
                for future in done:
                    id_, result = future.result()
                    items = resultHeapq.pushpop(HeapItem(id_, result))
                    for item in items:
                        self.repHandler(*item.obj)
                futureList = list(pending)
                if not self.finish:
                    futureList = futureList + \
                        self.producer(session, reqObjIter, self.thread - len(pending))
                if not futureList:
                    return

    def run(self, reqObjIter):
        asyncio.get_event_loop().run_until_complete(self.doRun(reqObjIter))


