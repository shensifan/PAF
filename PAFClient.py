#!/usr/bin/env python
# -*- coding: utf-8 -*-  
#TODO:没有超时处理
#     和PAFServer一样只处理TCP
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
  def __init__(self, callbackcount = 10):
    self.requestid = random.randrange(0,1000000000)
    #self.requestid = 0
    self.requestid_lock = threading.Lock()

    self.log = Util.Log(prefix = "PAFClient")

    #为了使用名字查找代理
    #self.proxy["server_name"][(ip, port)] = proxy
    self.proxy = {}
    #为了使用socket查找代理
    #self.connections[fileno] = proxy
    self.connections = {}
    #连接锁,用于对以上两个变量做同步操作
    self.connect_lock = threading.Lock()

    self.epoll = select.epoll()

    #response返回错误码,需要与PAFServer对应
    self.RESPONSE = dict()
    self.RESPONSE["E_OK"] = 0
    self.RESPONSE["E_CLOSE"] = -1 #标识需要断开链接
    self.RESPONSE["E_TIMEOUT"] = -2 #超时
    self.RESPONSE["E_CONNECTRESET"] = -3 #链接断开
    self.RESPONSE["E_UNKNOWN"] = -99  #不可知错误
    self.RESPONSE["E_WAIT"] = -10000    #初始化状态,正在等待服务端返回

    #同步请求使用的等待锁
    #self.request[requestid]["lock"] = threading.Lock()
    #请求所在链接
    #self.request[requestid]["connect"] = fileno
    #请求发送时间
    #self.request[requestid]["time"] = int(time.time())
    #异步回调函数
    #self.request[requestid]["callback"] = callback
    #请求返回,用于异步
    #self.request[requestid]["return"] = 0      #返回值,正常处理返回0,否则返回错误号
    #self.request[requestid]["message"] = ""    #错误信息
    #self.request[requestid]["result"] = None   #处理函数返回值
    self.request = {}
    self.request_lock = threading.Lock()

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
    if not self.requestid_lock.acquire():
      self.log.Print("require requestid lock error" + str(e))
      return random.randrange(0,1000000000)

    try:
      self.requestid += 1
      return self.requestid
    except BaseException, e:
      self.log.Print("release requestid lock error" + str(e))
      return random.randrange(0,1000000000)
    finally:
      self.requestid_lock.release()


  def put2Queue(self, requestid):
    if not self.queue_lock.acquire():
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
    if not self.queue_lock.acquire():
      self.log.Print("require lock error" + str(e))
      return None
    
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
    if not self.request_lock.acquire():
      self.log.Print("require lock error" + str(e))
      return -1

    try:
      if self.request.has_key(requestid):
        self.log.Print("request %d has exist" % requestid)
        return -1

      self.request[requestid] = dict()
      if callback == None: #同步调用,设置同步锁
        self.request[requestid]["lock"] = threading.Lock()
        if not self.request[requestid]["lock"].acquire():
          self.log.Print("require request lock error" + str(e))
          return -1
      else:
        self.request[requestid]["lock"] = None

      self.request[requestid]["connect"] = fileno
      self.request[requestid]["time"] = int(time.time())
      self.request[requestid]["callback"] = callback

      self.request[requestid]["return"] = self.RESPONSE["E_WAIT"]
      self.request[requestid]["message"] = ""
      self.request[requestid]["result"] = None
    except BaseException, e:
      self.log.Print("addRequest " + str(e))
      return -1
    finally:
      self.request_lock.release()

    return 0


  def delRequest(self, requestid):
    if not self.request_lock.acquire():
      self.log.Print("require lock error" + str(e))
      return None
      
    try:
      if self.request.has_key(requestid):
        del self.request[requestid]
      else:
        self.log.Print("request %d has gone" % requestid)
    finally:
      self.request_lock.release()


  def createProxy(self, name, endpoint):
    ''' 创建服务代理,可能会抛出异常 '''
    if not self.connect_lock.acquire():
      self.log.Print("require lock error" + str(e))
      return None

    try:
      if not self.proxy.has_key(name):
        self.proxy[name] = {}
      if not self.proxy[name].has_key(endpoint):
        self.proxy[name][endpoint] = ServerProxy(self, name, endpoint)
        self.proxy[name][endpoint].connect()

      return self.proxy[name][endpoint]
    finally:
      self.connect_lock.release()


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

    self.connection = None
    self.has_connect = False

    self.response_buffer = ""   #返回数据缓冲区

    #用于控制只有一个线程发送数据
    self.send_lock = threading.Lock()

    #保存已经发送的所有request,当链接断开时解锁上面的所有请求
    self.all_request = {}
    self.request_lock = threading.Lock()


  def disconnect(self, fileno):
    """ 这个不要加锁,在调用它时调用方加锁 """
    self.has_connect = False

    if self.client.connections.has_key(fileno):
      del self.client.connections[fileno]
    try:
      self.client.epoll.unregister(fileno)
    except:
      pass
      
    #释放所有request
    self.request_lock.acquire()
    try:
      for requestid in self.all_request:
        self.client.request[requestid]["return"] = self.client.RESPONSE["E_CONNECTRESET"]
        self.client.request[requestid]["message"] = " %d connect reset" % requestid
        if self.client.request[requestid]["callback"] == None: #同步调用
          self.client.request[requestid]["lock"].release()
        else: #异步调用
          self.client.put2Queue(requestid)
          #通知回调线程
          if self.client.condition.acquire():
            self.client.condition.notify()
            self.client.condition.release()

      #清空请求记录
      self.all_request.clear()
    finally:
      self.request_lock.release()


  def connect(self):
    """ 这个不要加锁,在调用它时调用方加锁 """
    if self.has_connect == True:
      return None

    try:
      self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      self.connection.connect(self.endpoint)
      self.connection.setblocking(0)

      self.response_buffer = ""
      self.client.connections[self.connection.fileno()] = self
      self.client.proxy[self.name][self.endpoint] = self

      self.client.epoll.register(self.connection.fileno(), select.EPOLLIN)
    except BaseException, e:
      self.has_connect = False
      raise e

    self.has_connect = True


  def _addRequest(self, requestid, callback):
    #print "add request %d" % requestid
    self.request_lock.acquire()
    try:
      self.client.addRequest(self.connection.fileno(), requestid, callback)
      self.all_request[requestid] = int(time.time())
    finally:
      self.request_lock.release()


  def delRequest(self, requestid):
    #print "del request %d" % requestid
    self.request_lock.acquire()
    try:
      self.client.delRequest(requestid)
      if self.all_request.has_key(requestid):
        del self.all_request[requestid]
    finally:
      self.request_lock.release()


  def _connect(self):
    self.client.connect_lock.acquire()
    try:
      self.connect()
    finally:
      self.client.connect_lock.release()


  def __request(self, methodname, params):
    """ 出错时会抛出异常 """
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
      self._addRequest(requestid, None)
    else:
      self._addRequest(requestid, callback)

    #加锁,保证发送数据报的完整
    if not self.send_lock.acquire():
      self.log.Print("require lock error" + str(e))
      raise BaseException, "require lock error"

    try:
      send_len = 0
      while send_len < len(finaldata):
        try:
          send_len += self.connection.send(finaldata[send_len:])
        except BaseException, e:
          if e[0] == 104: #链接已经断开
            self.log.Print("connect has disconnection")
            self._CloseConnect(item["connection"])
            break
          time.sleep(0.01)
    finally:
      self.send_lock.release()
    
    if async: #异步调用
      return True
    
    try:
      #同步调用,等待解锁
      if not self.client.request[requestid]["lock"].acquire():
        self.log.Print("require lock error" + str(e))
        raise BaseException, "require lock error"

      #调用出错
      if self.client.request[requestid]["return"] < 0:
        msg = self.client.request[requestid]["message"]
        raise BaseException, msg

      #正常返回
      return self.client.request[requestid]["result"]
    finally:
      self.delRequest(requestid)


  def __getattr__(self, name):
    return _Method(self.__request, name)


class EventThread(threading.Thread):
  """ 事件处理线程 """
  def setClient(self, client):
    self.client = client


  def _CloseConnect(self, fileno):
    if not self.client.connect_lock.acquire():
      self.client.log.Print("connecto lock acquire error")
      return None
    try:
      if not self.client.connections.has_key(fileno):
        self.client.log.Print("%d has closed" % fileno)
        return
      
      #disconnect中会执行 del self.client.connections[fileno]
      #不确实会不会有问题,使用一个临时变量保证对象可用
      proxy = self.client.connections[fileno]
      proxy.disconnect(fileno)
      self.client.log.Print("%d closed" % fileno)
    finally:
      self.client.connect_lock.release()


  def run(self):
    while True:
      events = self.client.epoll.poll(0.100)
      for fileno, event in events:
        try:
          if event & select.EPOLLIN: #数据信息
            if not self.client.connect_lock.acquire():
              self.client.log.Print("connecto lock acquire error")
              continue
            try:
              if not self.client.connections.has_key(fileno):
                self.client.log.Print("connect has gone")
                continue
              proxy = self.client.connections[fileno]
            finally:
              self.client.connect_lock.release()

            msg = proxy.connection.recv(10 * 1024)

            if len(msg) <= 0:#链接断开
              self._CloseConnect(fileno)
              continue

            proxy.response_buffer += msg
            #数据分包
            while True:
              if len(proxy.response_buffer) <= 4:
                break
              #读取前四字节,包长度
              length = struct.unpack("@I", proxy.response_buffer[0:4])[0]
              if len(proxy.response_buffer) < 4 + length:
                break

              response = cPickle.loads(proxy.response_buffer[4:(length+4)])
              proxy.response_buffer = proxy.response_buffer[4+length:]

              requestid = response["requestid"]

              #处理数据
              if not self.client.request_lock.acquire():
                self.client.log.Print("request lock error")
                continue

              try:
                if not self.client.request.has_key(requestid):
                  self.client.log.Print("request %d has gone"%requestid)
                  continue

                self.client.request[requestid]["return"] = response["return"]
                if response["return"] < 0:
                  self.client.request[requestid]["message"] = response["message"]
                else:
                  self.client.request[requestid]["result"] = response["result"]

                #同步调用
                if self.client.request[requestid]['callback'] == None:
                  self.client.request[requestid]["lock"].release()
                else:
                  self.client.put2Queue(requestid)
                  #通知回调线程
                  if self.client.condition.acquire():
                    self.client.condition.notify()
                    self.client.condition.release()
              finally:
                self.client.request_lock.release()

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
      if not self.client.condition.acquire():
        self.client.log.Print("condition acquire error")
        continue

      self.client.condition.wait()
      self.client.condition.release()
          
      while True:
        item = self.client.getFromQueue()
        if item == None:
          break

        #处理数据
        if not self.client.request_lock.acquire():
          self.client.log.Print("request lock error")
          continue

        try:
          if not self.client.request.has_key(item):
            self.client.log.Print("this request has gone")
            continue
          request = self.client.request[item]
        finally:
          self.client.request_lock.release()

        try:

          #同步调用
          if request["callback"] == None: 
            continue

          #异步调用
          try:
            request["callback"].callback((request["return"], request["message"]), request["result"])
          except BaseException, e:
            tb = sys.exc_info()[2]
            while tb is not None:
              f = tb.tb_frame
              lineno = tb.tb_lineno
              co = f.f_code
              filename = co.co_filename
              tb = tb.tb_next
            self.client.log.Print("callback thread error %s:%d %s " % (filename, lineno, str(e)))
          finally:
            if not self.client.connect_lock.acquire():
              self.client.log.Print("connecto lock acquire error")
              return None
            try:
              if self.client.connections.has_key(request["connect"]):
                self.client.connections[request["connect"]].delRequest(item)
            finally:
              self.client.connect_lock.release()

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

  client = PAFClient()
  #createProxy 可能会抛出异常
  try:
    t = client.createProxy("test", ('127.0.0.1', 8412))
  except BaseException, e:
    print e
    exit(0)
  try:  #request 1
    print t.nofun(" world")
  except BaseException, e:
    print e

  try:  #request 2
    print t.sayHello(" world")
  except BaseException, e:
    print e

  try:  #request 3
    print t.sayHello(" world", " sync")
  except BaseException, e:
    print e

  try:  #request 4
    t.async_nofun(" world", callback_sayHello())
  except BaseException, e:
    print e

  try:  #request 5
    t.async_sayHello(" world", callback_sayHello())
  except BaseException, e:
    print e

  try:  #request 6
    t.async_sayHello(" world", " async", callback_sayHello())
  except BaseException, e:
    print e

  try:  #request 7
    t.throwException()
  except BaseException, e:
    print e

  try:  #request 8
    t.testTimeout()
  except BaseException, e:
    print e


  #要等待异步返回
  try:
    time.sleep(100)
  except BaseException, e:
    print str(e)
