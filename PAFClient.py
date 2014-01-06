#!/usr/bin/env python
# -*- coding: utf-8 -*-  
#TODO:没有超时处理,超时的请求会一直收不到回复
#     如果是同步线程会卡死同步线程
#     如果是异步线程会多占用一定内存,request一直不能清除
#     当链接断开时需要解锁上面已经发出的请求
#     加入重连机制
import os
import sys
import socket
import select
import Queue
import thread
import time
import logging
import struct
import cPickle
import inspect
import threading
import copy

class PAFClient():
  def __init__(self, callbackcount = 3):
    self.requestid = 0

    #到其它服务的链接
    #self.proxy["server_name"][(ip, port)] = proxy
    self.proxy = {}
    self.response_buffer = {}

    self.epoll = select.epoll()

    #请求字典
    #self.request[requestid] = 
    self.request = {}
    self.connections = {}

    #事件线程与回调线程队列
    self.response_queue = Queue.Queue(maxsize = 10000)
    self.queue_lock = threading.Lock()
    self.condition = threading.Condition()

    #回调线程
    self.callbacks = []
    self.callback_count = callbackcount
    index = 0
    while index < callbackcount:
      self.callbacks.append(CallBackThread())
      self.callbacks[index].setClient(self)
      self.callbacks[index].setDaemon(True)
      self.callbacks[index].start()
      index += 1

    #启动事件处理线程
    self.event_thread = EventThread()
    self.event_thread.setClient(self)
    self.event_thread.setDaemon(True)
    self.event_thread.start()


  def getRequestId(self):
    self.requestid += 1
    return self.requestid


  def put2Queue(self, request_id):
    try:
      self.queue_lock.acquire()
    except BaseException, e:
      print("require lock error" + str(e))
      return -1
    
    ret = 0
    try:
      self.response_queue.put(request_id)
    except BaseException, e:
      print("put2Queue " + str(e))
      ret = -1

    self.queue_lock.release()
    return ret


  def getFromQueue(self):
    try:
      self.queue_lock.acquire()
    except BaseException, e:
      print("require lock error" + str(e))
      return None
    
    data = None
    try:
      if not self.response_queue.empty():
        data = self.response_queue.get()
    except BaseException, e:
      print("getFromQueue error" + str(e))

    self.queue_lock.release()
    return data


  def addRequest(self, requestId, callback):
    """ 异步发送请求,把相关信息加入字典,返回时使用 """
    try:
      item = dict()
      item["lock"] = threading.Lock()
      item["lock"].acquire()
      item["callback"] = callback
      item["data"] = None
      item["return"] = 0
      item["message"] = ""
      self.request[requestId] = item
    except BaseException, e:
      print("addRequest " + str(e))
      return -1

    return 0


  def createProxy(self, name, endpoint):
    #TODO:当endpoint为None随机返回一个
    #会抛出异常
    if not self.proxy.has_key(name):
      self.proxy[name] = {}

    if not self.proxy[name].has_key(endpoint):
      self.proxy[name][endpoint] = ServerProxy(self, endpoint)

    return self.proxy[name][endpoint]


class _Method:
  def __init__(self, send, name):
    self.__send = send
    self.__name = name

  def __getattr__(self, name):
    return _Method(self.__send, "%s.%s" % (self.__name, name))

  def __call__(self, *args):
    return self.__send(self.__name, args)


class ServerProxy:
  def __init__(self, client, endpoint):
    self.client = client
    self.connect = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.connect.connect(endpoint)
    self.connect.setblocking(0)

    self.client.connections[self.connect.fileno()] = self.connect
    self.client.response_buffer[self.connect.fileno()] = ""
    self.client.epoll.register(self.connect.fileno(), select.EPOLLIN)


  def __request(self, methodname, params):
    name = methodname
    async = False

    if methodname[0:6] == "async_":
      name = methodname[6:]
      async = True
      callback = params[len(params)-1]
      params = params[0:len(params)-1]

    result = None
    requestid = self.client.getRequestId()
    request = {}
    request["requestid"] = requestid
    request["fun"] = name
    request["data"] = cPickle.dumps(params)

    temp_data = cPickle.dumps(request)

    length = len(temp_data)
    finaldata = struct.pack("@I", length)
    finaldata += temp_data
    if not async:
      self.client.addRequest(requestid, None)
    else:
      self.client.addRequest(requestid, callback)
    self.connect.send(finaldata)

    if async: #异步调用
      return True
    else:#同步调用
      self.client.request[requestid]["lock"].acquire()
      if self.client.request[requestid]["return"] < 0:
        msg = self.client.request[requestid]["message"]
        del self.client.request[requestid]
        raise NameError, msg
      result = cPickle.loads(self.client.request[requestid]["data"])
      del self.client.request[requestid]

    return result


  def __getattr__(self, name):
    #print "name="+name
    return _Method(self.__request, name)


class EventThread(threading.Thread):
  """ 事件处理线程 """
  def setClient(self, client):
    self.client = client

  def _CloseConnect(self, fileno):
    self.client.epoll.unregister(fileno)
    self.client.connections[fileno].close()
    del self.client.connections[fileno]
    del self.client.response_buffer[fileno]
    print("%d closed" % fileno)


  def run(self):
    while True:
        events = self.client.epoll.poll(100)
        for fileno, event in events:
          try:
            if event & select.EPOLLIN: #数据信息
              msg = self.client.connections[fileno].recv(65535)

              if len(msg) <= 0:#链接断开
                self._CloseConnect(fileno)
                continue

              self.client.response_buffer[fileno] += msg
              #数据分包
              while True:
                if len(self.client.response_buffer[fileno]) <= 4:
                  break
                #读取前四字节,包长度
                length = struct.unpack("@I", self.client.response_buffer[fileno][0:4])[0]
                if len(self.client.response_buffer[fileno]) < 4 + length:
                  break

                response = cPickle.loads(self.client.response_buffer[fileno][4:(length+4)])
                requestid = response["requestid"]
                self.client.request[requestid]["return"] = response["return"]
                if response["return"] < 0:
                  self.client.request[requestid]["message"] = response["message"]
                else:
                  self.client.request[requestid]["data"] = response["result"]
                self.client.put2Queue(requestid)
                self.client.response_buffer[fileno] = self.client.response_buffer[fileno][4+length:]

                #通知回调线程
                while True:
                  if self.client.condition.acquire():
                    self.client.condition.notify()
                    self.client.condition.release()
                    break
            elif evnet & select.EPOLLHUP: #链接断开
              self._CloseConnect(fileno)
          except BaseException, e:
            self._CloseConnect(fileno)
            print "Event thread error " + str(e)



class CallBackThread(threading.Thread):
  """ 异步回调线程 """
  def setClient(self, client):
    self.client = client

  def run(self):
    while True:
      try:
        if self.client.condition.acquire():
          self.client.condition.wait()
          self.client.condition.release()
          
        while True:
          item = self.client.getFromQueue()
          if item == None:
            break

          if not self.client.request.has_key(item):
            print "this request has gone"
            continue

          request = self.client.request[item]
          if request["callback"] == None: #同步调用
            request["lock"].release()
          else:   #异步调用
            if request["return"] < 0:
              request["callback"].callback((request["return"], request["message"]), None)
            else:
              request["callback"].callback((request["return"], request["message"]), cPickle.loads(request["data"]))
      except BaseException, e:
        print "callback thread error " + str(e)



if __name__ == "__main__":
  class callback_sayHello():
    """ callback类,需要实现callback函数  
        def callback(self, ret, result)

        ret = (return_code, message)
        return_code = 0: 成功
        return_code < 0: 失败,错误信息在message中
        result = 函数返回
    """
    """ callback为什么要设计成类
        因为在服务端异步调用其它服务时,需要保存链接信息等用于返回给客户端
        这个类需要业务自己实现
    """
    def callback(self, ret, result):
      if ret[0] < 0:
        print ret[1]
      else:
        print result

  client = PAFClient()
  #createProxy 可能会抛出异常
  try:
    t = client.createProxy("test", ('127.0.0.1', 8412))
  except BaseException, e:
    print str(e)
    exit(0)

  try:
    print t.nofun(" world")
  except BaseException, e:
    print str(e)

  try:
    print t.sayHello(" world")
  except BaseException, e:
    print str(e)

  try:
    print t.sayHello(" world", " sync")
  except BaseException, e:
    print str(e)

  try:
    print t.async_nofun(" world", callback_sayHello())
  except BaseException, e:
    print str(e)

  try:
    print t.async_sayHello(" world", callback_sayHello())
  except BaseException, e:
    print str(e)

  try:
    print t.async_sayHello(" world", " async", callback_sayHello())
  except BaseException, e:
    print str(e)

  #要等待异步返回
  try:
    time.sleep(100)
  except BaseException, e:
    print str(e)
