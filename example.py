from asynchttpclient import AsyncHttpClient, RequestObject
from typing import NamedTuple, Generator


querys = ['china', 'japan', 'america']


def reqObjGenerator() -> Generator[RequestObject, None, None]:
    for query in querys:
        reqObj = RequestObject(
            url='http://www.baidu.com/s?wd=%s' % query)
        yield reqObj


def repHandler(reqObj: RequestObject, resp: str=None, respBody: bytes=None):
    ret = reqObj.url
    print(ret)
    if respBody:
        print(respBody.decode('utf8'))


client = AsyncHttpClient(method='get', repHandler=repHandler, thread=10)
client.run(reqObjGenerator())
