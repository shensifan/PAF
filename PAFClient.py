#!/usr/bin/env python
# -*- coding: utf-8 -*-  
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

class PAFClient():
  def __init__(self, callbackcount = 3, timeout = 60):
    #self.requestid = random.randrange(0,1000000000)
    self.requestid = 0

    self.log = Util.Log(logfile="PAFClient.log", prefix = "PAFClient")

    #self.proxy["server_name"][(ip, port)] = proxy
    self.proxy = {}
    #self.connections[fileno] = proxy
    self.connections = {}
    #self.response_buffer[fileno] = ""
    self.response_buffer = {}

    self.epoll = select.epoll()

    #response返回错误码,需要与PAFServer对应
    self.RESPONSE = dict()
    self.RESPONSE["E_OK"] = 0
    self.RESPONSE["E_CLOSE"] = -1 #标识需要断开链接
    self.RESPONSE["E_TIMEOUT"] = -2 #超时
    self.RESPONSE["E_CONNECTRESET"] = -3 #链接断开
    self.RESPONSE["E_UNKNOWN"] = -99  #不可知错误

    #超时时间30秒
    self.timeout = timeout

    #同步请求使用的等待锁
    #self.request[requestid]["lock"] = threading.Lock()
    #请求所在链接
    #self.request[requestid]["connect"] = fileno
    #请求发送时间
    #self.request[requestid]["time"] = int(time.time())
    #异步回调函数
    #self.request[requestid]["callback"] = callback
    #请求返回,用于异步
    #self.request[requestid]["return"] = 0
    #self.request[requestid]["message"] = ""
    #self.request[requestid]["result"] = None
    self.request = {}

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

  def put2Queue(self, requestid):
    try:
      self.queue_lock.acquire()
    except BaseException, e:
      self.log.Print("require lock error" + str(e))
      return -1
    
    try:
      self.response_queue.put(requestid)
    except BaseException, e:
      self.log.Print("put2Queue " + str(e))
      return -1
    finally:
      self.queue_lock.release()

    return 0


  def getFromQueue(self):
    try:
      self.queue_lock.acquire()
    except BaseException, e:
      self.log.Print("require lock error" + str(e))
      return None
    
    data = None
    try:
      if not self.response_queue.empty():
        return self.response_queue.get()
    except BaseException, e:
      self.log.Print("getFromQueue error" + str(e))
      return None
    finally:
      self.queue_lock.release()

    return None


  def addRequest(self, fileno, requestid, callback):
    """ 异步发送请求,把相关信息加入字典,返回时使用 """
    try:
      self.request[requestid] = dict()
      if callback == None: #同步调用
        self.request[requestid]["lock"] = threading.Lock()
        self.request[requestid]["lock"].acquire()
      else:
        self.request[requestid]["lock"] = None

      self.request[requestid]["connect"] = fileno
      self.request[requestid]["time"] = int(time.time())
      self.request[requestid]["callback"] = callback

      self.request[requestid]["return"] = -99
      self.request[requestid]["message"] = ""
      self.request[requestid]["result"] = None
    except BaseException, e:
      self.log.Print("addRequest " + str(e))
      return -1

    return 0


  def createProxy(self, name, endpoint):
    if not self.proxy.has_key(name):
      self.proxy[name] = {}

    if not self.proxy[name].has_key(endpoint):
      self.proxy[name][endpoint] = ServerProxy(self, name, endpoint)

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
  def __init__(self, client, name, endpoint):
    self.client = client
    self.name = name
    self.endpoint = endpoint
    self.old_fileno = -1

    self.has_connect = False

    #用于控制只有一个线程发送数据
    self.send_lock = threading.Lock()


  def _connect(self):
    if self.has_connect == True:
      return 0

    if self.old_fileno > 0:
      del self.client.response_buffer[self.old_fileno]
      del self.client.connections[self.old_fileno]

    try:
      self.connect = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      self.connect.connect(self.endpoint)
      self.connect.setblocking(0)

      self.client.response_buffer[self.connect.fileno()] = ""
      self.client.connections[self.connect.fileno()] = self
      self.client.proxy[self.name][self.endpoint] = self
      self.old_fileno = self.connect.fileno()
      self.client.epoll.register(self.connect.fileno(), select.EPOLLIN)
    except BaseException, e:
      self.has_connect = False
      raise e

    self.has_connect = True
    return 0


  def __request(self, methodname, params):
    self._connect()

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
    request["pargma"] = params

    temp_data = cPickle.dumps(request)

    length = len(temp_data)
    finaldata = struct.pack("@I", length)
    finaldata += temp_data
    if not async:
      self.client.addRequest(self.connect.fileno(), requestid, None)
    else:
      self.client.addRequest(self.connect.fileno(), requestid, callback)
    
    #加锁,保证发送数据报的完整
    self.send_lock.acquire()
    try:
      self.connect.sendall(finaldata)
    except BaseException, e:
      self.client.log.Print("send error %s" % str(e))
      #重连后重新发送一次
      self.has_connect = False
      self._connect()
      self.connect.sendall(finaldata)
    finally:
      self.send_lock.release()
    
    if async: #异步调用
      return True
    
    #同步调用,等待解锁
    self.client.request[requestid]["lock"].acquire()

    try:
      #调用出错
      if self.client.request[requestid]["return"] < 0:
        msg = self.client.request[requestid]["message"]
        raise BaseException, msg

      #正常返回
      return self.client.request[requestid]["result"]
    finally:
      del self.client.request[requestid]


  def __getattr__(self, name):
    return _Method(self.__request, name)


class EventThread(threading.Thread):
  """ 事件处理线程 """
  def setClient(self, client):
    self.client = client


  def _CloseConnect(self, fileno):
    if not self.client.connections.has_key(fileno):
      self.client.log.Print("%d has closed" % fileno)
      return

    self.client.epoll.unregister(fileno)
    #只是把proxy设置为断开链接,并没有删除
    self.client.connections[fileno].has_connect = False
    del self.client.connections[fileno]
    del self.client.response_buffer[fileno]
    self.client.log.Print("%d closed" % fileno)


  def _timeOut(self):
    """ 处理request超时 """
    now = int(time.time())
    for requestid in self.client.request:
      if self.client.request[requestid]["return"] != -99: #已经在处理这个请求了,不要再处理了
        continue

      try:
        c = self.client.request[requestid]["connect"] 
        t = self.client.request[requestid]["time"]

        delete = False
        if not self.client.connections.has_key(c):
          self.client.request[requestid]["return"] = self.client.RESPONSE["E_CONNECTRESET"]
          self.client.request[requestid]["message"] = "connect reset"
          self.client.log.Print("connect has gone del request %d" % requestid)
          delete = True

        if (now - t) > self.client.timeout:
          self.client.request[requestid]["return"] = self.client.RESPONSE["E_TIMEOUT"]
          self.client.request[requestid]["message"] = "request time out" 
          self.client.log.Print("connect has timeout del request %d" % requestid)
          delete = True

        if delete == False:
          return

        self.client.put2Queue(requestid)
        #通知回调线程
        self.client.condition.acquire()
        self.client.condition.notify()
        self.client.condition.release()
      except BaseException, e:
        self.client.log.Print("time out exception " + str(e))


  def run(self):
    while True:
      self._timeOut()

      events = self.client.epoll.poll(0.100)
      for fileno, event in events:
        try:
          if event & select.EPOLLIN: #数据信息
            msg = self.client.connections[fileno].connect.recv(10 * 1024)

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
              #可能已经被超时删除了
              if self.client.request.has_key(requestid):
                self.client.request[requestid]["return"] = response["return"]
                if response["return"] < 0:
                  self.client.request[requestid]["message"] = response["message"]
                else:
                  self.client.request[requestid]["result"] = response["result"]
                self.client.put2Queue(requestid)

              self.client.response_buffer[fileno] = self.client.response_buffer[fileno][4+length:]

              #通知回调线程
              self.client.condition.acquire()
              self.client.condition.notify()
              self.client.condition.release()

          if event & select.EPOLLHUP: #链接断开
            self._CloseConnect(fileno)

        except BaseException, e:
          self._CloseConnect(fileno)
          traceback.print_exc()
          self.client.log.Print("Event thread error " + str(e))



class CallBackThread(threading.Thread):
  """ 异步回调线程 """
  def setClient(self, client):
    self.client = client

  def run(self):
    while True:
      try:
        self.client.condition.acquire()
        self.client.condition.wait()
        self.client.condition.release()
      except BaseException, e:
        self.client.log.Print("callback thread condition error " + str(e))
        continue
          
      while True:
        item = self.client.getFromQueue()
        if item == None:
          break

        if not self.client.request.has_key(item):
          self.client.log.Print("this request has gone")
          del self.client.request[item]
          continue

        try:
          request = self.client.request[item]

          #同步调用
          if request["callback"] == None: 
            request["lock"].release()
            continue

          #异步调用
          request["callback"].callback((request["return"], request["message"]), request["result"])
          del self.client.request[item]
        except BaseException, e:
          self.client.log.Print("callback thread error " + str(e))


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

  client = PAFClient(timeout=3)
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
    t.async_nofun(" world", callback_sayHello())
  except BaseException, e:
    print str(e)

  try:
    t.async_sayHello(" world", callback_sayHello())
  except BaseException, e:
    print str(e)

  try:
    t.async_sayHello(" world", " async", callback_sayHello())
  except BaseException, e:
    print str(e)

  try:
    t.testTimeout()
  except BaseException, e:
    print str(e)

  try:
    t.throwException()
  except BaseException, e:
    print str(e)





  #要等待异步返回
  try:
    time.sleep(100)
  except BaseException, e:
    print str(e)
