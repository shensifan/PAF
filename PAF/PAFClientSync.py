#!/usr/bin/env python
# -*- coding: utf-8 -*-  
#使用PAF协议,但只有一个线程的同步操作
import os
import sys
import socket
import select
import Queue
import thread
import time
import struct
import cPickle
import threading
import random
import traceback
sys.path.insert(0, "%s/.." % os.path.dirname(__file__))
import Util

class _Method:
    def __init__(self, send, name):
        self.__send = send
        self.__name = name

    def __getattr__(self, name):
        return _Method(self.__send, "%s.%s" % (self.__name, name))

    def __call__(self, *args):
        return self.__send(self.__name, args)


class PAFClientSync():
    def __init__(self, endpoint):
        self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connection.connect(endpoint)

    def __request(self, methodname, params):
        """ 出错时会抛出异常 """
        name = methodname

        result = None
        requestid = random.randrange(0, 1000000000)
        request = {}
        request["requestid"] = requestid
        request["fun"] = name
        request["pargma"] = params

        temp_data = cPickle.dumps(request)

        length = len(temp_data)
        finaldata = struct.pack("@I", length)
        finaldata += temp_data

        send_len = 0
        while send_len < len(finaldata):
            try:
                send_len += self.connection.send(finaldata[send_len:])
            except BaseException as e:
                if e[0] == 104: #链接已经断开
                    raise BaseException("send error connection closed")
                time.sleep(0.01)

        while True:
            response_buffer = ""
            try:
                msg = self.connection.recv(10 * 1024)
            except:
                msg = ''

            if len(msg) <= 0:#链接断开
                raise BaseException("recv error")

            response_buffer += msg
            if len(response_buffer) <= 4:
                time.sleep(0.01)
                continue

            #读取前四字节,包长度
            length = struct.unpack("@I", response_buffer[0:4])[0]
            if len(response_buffer) < 4 + length:
                time.sleep(0.01)
                continue

            #处理返回结果
            response = cPickle.loads(response_buffer[4:(length + 4)])
            if response["return"] < 0:
                raise BaseException(response["message"])
            
            return response["result"]

    def __getattr__(self, name):
        return _Method(self.__request, name)


if __name__ == "__main__":
    t = PAFClientSync(('127.0.0.1', 8412))
    print t.sayHello(" world", " sync")
