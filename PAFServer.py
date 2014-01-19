#!/usr/bin/env python
# -*- coding: utf-8 -*-  
#TODO:正常退出机制
import os
import sys
import socket
import select
import Queue
import thread
import time
import threading
import struct
import cPickle
import copy
import PAFClient
import traceback
import Util

class PAFServer():
  def __init__(self, obj, workcount):
    try:
      self.log = Util.Log(logfile="PAFServer.log", prefix = "PAFServer")

      self.epoll = select.epoll()

      #response返回错误码
      self.RESPONSE = dict()
      self.RESPONSE["E_OK"] = 0
      self.RESPONSE["E_CLOSE"] = -1 #标识需要断开链接
      self.RESPONSE["E_UNKNOWN"] = -99  #不可知错误

      #监听server
      self.server = None

      #所有链接
      self.connections = {}
      self.requests_buffer = {}

      #客户端,供服务对象调用其它服务使用
      self.client = PAFClient.PAFClient(5)

      #服务对象
      self.obj = obj

      #主线程与工作线程通信队列
      self.request_queue = Queue.Queue(maxsize = 100000)
      self.request_lock = threading.Lock()
      self.request_condition = threading.Condition()

      #工作线与主线程程通信队列
      self.response_queue = Queue.Queue(maxsize = 100000)
      self.response_lock = threading.Lock()
      
      #工作线程
      self.workers = []
      self.work_count = workcount
      index = 0
      while index < workcount:
        self.workers.append(WorkThread())
        self.workers[index].setObject(obj)
        self.workers[index].setServer(self)
        self.workers[index].setDaemon(True)
        index += 1
    except BaseException, e:
      self.log.Print("init error " + str(e))
      exit(0)


  def init(self, ip, port, stype = socket.SOCK_STREAM):
    try:
      self.server = socket.socket(socket.AF_INET, stype)
      self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      self.server.bind((ip, port))
      self.server.listen(10000)
      self.server.setblocking(0)
      self.epoll.register(self.server.fileno(), select.EPOLLIN)
    except BaseException, e:
      self.log.Print("init error " + str(e))
      exit(0)


  def addRequest(self, fileno, data):
    try:
      self.request_lock.acquire()
    except BaseException, e:
      self.log.Print("require lock error" + str(e))
      return -1
    
    try:
      request = cPickle.loads(data)
    except BaseException, e:
      self._CloseConnect(fileno)
      self.log.Print("loads data error " + str(e))
      return -1

    try:
      item = dict()
      item["connection"] = fileno
      item["requestid"] = request["requestid"]
      item["fun"] = request["fun"]
      current = dict()
      current.update(current)
      item["pargma"] = list(request["pargma"])
      item["pargma"].append(current)
      self.request_queue.put(item)
    except BaseException, e:
      self._CloseConnect(fileno)
      self.log.Print("loads data error " + str(e))
      return -1
    finally:
      self.request_lock.release()

    return 0


  def getRequest(self):
    try:
      self.request_lock.acquire()
    except BaseException, e:
      self.log.Print("require lock error" + str(e))
      return None
    
    try:
      if not self.request_queue.empty():
        return self.request_queue.get()
    except BaseException, e:
      self.log.Print("error" + str(e))
      return None
    finally:
      self.request_lock.release()

    return None


  def addResponse(self, fileno, requestid, ret, message, result):
    try:
      self.response_lock.acquire()
    except BaseException, e:
      self.log.Print("require lock error" + str(e))
      return -1
    
    try:
      item = dict()
      item["requestid"] = requestid
      item["return"] = ret
      item["message"] = message
      item["connection"] = fileno
      item["result"] = result
      self.response_queue.put(item)
    except BaseException, e:
      self.log.Print("error" + str(e))
      return -1
    finally:
      self.response_lock.release()

    return 0


  def getResponse(self):
    try:
      self.response_lock.acquire()
    except BaseException, e:
      self.log.Print("require lock error" + str(e))
      return None
    
    try:
      if not self.response_queue.empty():
        return self.response_queue.get()
    except BaseException, e:
      self.log.Print("error" + str(e))
      return None
    finally:
      self.response_lock.release()

    return None


  def _CloseConnect(self, fileno):
    try:
      self.epoll.unregister(fileno)
      self.connections[fileno].close()
      del self.connections[fileno]
      del self.requests_buffer[fileno]
      self.log.Print("%d closed" % fileno)
    except BaseException, e:
      self.log.Print("close connect %d error %s" % (fileno, str(e)))


  def _accept(self):
    try:
      connection, address = self.server.accept()
      connection.setblocking(0)
      self.epoll.register(connection.fileno(), select.EPOLLIN)
      self.connections[connection.fileno()] = connection
      self.requests_buffer[connection.fileno()] = b''
      self.log.Print("new connect %d from %s" % (connection.fileno(), str(address)))
    except BaseException, e:
      self.log.Print("accept error " + str(e))


  def _recv(self, fileno):
    try:
      msg = self.connections[fileno].recv(1024 * 100)
      if len(msg) <= 0:
        self._CloseConnect(fileno)
        return -1

      self.requests_buffer[fileno] += msg
      #数据分包
      while True:
        if len(self.requests_buffer[fileno]) <= 4:
          break
        #读取前四字节,包长度
        length = struct.unpack("@I", self.requests_buffer[fileno][0:4])[0]
        if len(self.requests_buffer[fileno]) < 4 + length:
          break
        if self.addRequest(fileno, self.requests_buffer[fileno][4:(length+4)]) == 0:
          self.requests_buffer[fileno] = self.requests_buffer[fileno][4+length:]
    except BaseException, e:
      self.log.Print("recv error %s" % str(e))
      self._CloseConnect(fileno)
      return -1

    return 0


  def _response(self):
    while True:
      item = self.getResponse()
      if item == None:
        return 0

      if item["result"] == self.RESPONSE["E_CLOSE"]:
        self._CloseConnect(item["connection"])

      if not self.connections.has_key(item["connection"]):
        self.log.Print("%d connect has gone" % item["connection"])
        continue

      try:
        temp_data = cPickle.dumps(item)
        length = len(temp_data)
        finaldata = struct.pack("@I", length)
        finaldata += temp_data
        self.connections[item["connection"]].sendall(finaldata)
      except BaseException, e:
        if e[0] == 104: #链接已经断开
          self._CloseConnect(item["connection"])
        self.log.Print("send error %s" % str(e))
        continue


  def start(self):
    #启动工作线程
    index = 0
    try:
      while index < self.work_count:
        self.workers[index].start()
        index += 1
    except BaseException, e:
      self.log.Print("start error " + str(e))
      exit(0)

    while True:
      #处理需要返回的数据
      self._response()

      try:
        events = self.epoll.poll(0.100)
      except BaseException, e:
        self.log.Print("epoll error " + str(e))
        time.sleep(0.010)
        continue

      for fileno, event in events:
        if fileno == self.server.fileno(): #监听服务信息
          self._accept()
          continue

        if event & select.EPOLLIN: #数据信息
          if self._recv(fileno) < 0:
            continue

          #通知工作线程
          try:
            self.request_condition.acquire()
            self.request_condition.notifyAll()
            self.request_condition.release()
          except BaseException, e:
            self.log.Print("notify to work thread error " + str(e))

        if event & select.EPOLLHUP: #链接断开
          self._CloseConnect(fileno)


class WorkThread(threading.Thread):
  def setObject(self, obj):
    self.obj = obj 

  def setServer(self, server):
    self.server = server

  """ 工作线程 """
  def run(self):
    obj = copy.deepcopy(self.obj)
    obj.setServer(self.server)

    #如果存在init函数，调用
    try:
      if not obj.init():
        self.server.log.Print("obj init error")
        exit(0)
    except BaseException, e:
      pass

    while True:
      try:
        self.server.request_condition.acquire()
        self.server.request_condition.wait()
        self.server.request_condition.release()
      except BaseException, e:
        self.server.log.Print("condition wait error " + str(e))
        continue

      while True:
        item = self.server.getRequest()
        if item == None:
          break

        try:
          method = getattr(obj, item["fun"])
        except BaseException, e:
          self.server.addResponse(item["connection"], item["requestid"], self.server.RESPONSE["E_UNKNOWN"], str(e), "")
          continue

        try:
          #temp_line_no = sys._getframe().f_lineno + 1  #!!!!得到下一行行号,与下一行之前不能有空行
          result = method(*item["pargma"])
        except BaseException, e:
          tb = sys.exc_info()[2]
          while tb is not None:
            f = tb.tb_frame
            lineno = tb.tb_lineno
            co = f.f_code
            filename = co.co_filename
            tb = tb.tb_next

          self.server.addResponse(item["connection"], item["requestid"], self.server.RESPONSE["E_UNKNOWN"], "%s:%d %s" % (filename, lineno, str(e)), "")
          continue

        if result == None:
          continue
        self.server.addResponse(item["connection"], item["requestid"], self.server.RESPONSE["E_OK"], "", result)


if __name__ == "__main__":
  #类实现限制:
  #1.所有参数都为输入参数，不能做为输出,输出通过返回值获得
  #2.函数返回None表示不需要给客户端返回数据
  #3.需要实现setserver接口
  #4.所有对外提供的接口需要以current参数为最后一个参数,这个参数为系统使用,不是接口参数
  class Test():
    def setServer(self, server):
      self.server = server

    def sayHello(self, data, data2, current):
      data = "hello" + data + data2
      result = dict()
      result["result"] = 1
      result["data"] = data
      return result

    def testTimeout(self, current):
      time.sleep(5)
      return "timeout"

    def throwException(self, current):
      raise BaseException, "a exception"


  server = PAFServer(Test(), 3)
  server.init("127.0.0.1", 8412)
  server.start()
