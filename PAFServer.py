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
import logging
import threading
import struct
import cPickle
import PAFClient
import copy

class PAFServer():
  def __init__(self, obj, workcount):
    self.epoll = select.epoll()
    #监听server
    self.server = None
    #所有链接
    self.connections = {}
    self.requests_buffer = {}

    #客户端为了在服务代码中调用其它服务
    self.client = PAFClient.PAFClient()

    #服务对象
    self.obj = obj

    #主线程与工作线程通信队列
    self.request_queue = Queue.Queue(maxsize = 100000)
    self.queue_lock = threading.Lock()
    self.condition = threading.Condition()
    
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


  def init(self, ip, port, stype):
    #server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
      self.server = socket.socket(socket.AF_INET, stype)
      self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      self.server.bind((ip, port))
      self.server.listen(10000)
      self.server.setblocking(0)
      self.epoll.register(self.server.fileno(), select.EPOLLIN)
    except BaseException, e:
      print "init error " + str(e)
      exit(0)


  def put2Queue(self, connection, data):
    try:
      self.queue_lock.acquire()
    except BaseException, e:
      print("require lock error" + str(e))
      return -1
    
    ret = 0
    try:
      item = dict()
      item["connection"] = connection
      item["data"] = data
      self.request_queue.put(item)
    except BaseException, e:
      print("error" + str(e))
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
      if not self.request_queue.empty():
        data = self.request_queue.get()
    except BaseException, e:
      print("error" + str(e))

    self.queue_lock.release()
    return data


  def _CloseConnect(self, fileno):
    self.epoll.unregister(fileno)
    self.connections[fileno].close()
    del self.connections[fileno]
    del self.requests_buffer[fileno]
    print("%d closed" % fileno)


  def start(self):
    #启动工作线程
    index = 0
    while index < self.work_count:
      self.workers[index].start()
      index += 1

    try:
      while True:
        events = self.epoll.poll(100)
        for fileno, event in events:
          try:
            if fileno == self.server.fileno(): #监听服务信息
              connection, address = self.server.accept()
              connection.setblocking(0)
              self.epoll.register(connection.fileno(), select.EPOLLIN)
              self.connections[connection.fileno()] = connection
              self.requests_buffer[connection.fileno()] = b''
              print "new connect %d" % connection.fileno()
            elif event & select.EPOLLIN: #数据信息
              msg = self.connections[fileno].recv(65535)
              if len(msg) <= 0:#链接断开
                print "msg length %d" % len(msg)
                self._CloseConnect(fileno)
                continue

              self.requests_buffer[fileno] += msg
              #数据分包
              while True:
                if len(self.requests_buffer[fileno]) <= 4:
                  break
                #读取前四字节,包长度
                length = struct.unpack("@I", self.requests_buffer[fileno][0:4])[0]
                if len(self.requests_buffer[fileno]) < 4 + length:
                  break
                self.put2Queue(self.connections[fileno], self.requests_buffer[fileno][4:(length+4)])
                self.requests_buffer[fileno] = self.requests_buffer[fileno][4+length:]
                #通知工作线程
                while True:
                  if self.condition.acquire():
                    self.condition.notify()
                    self.condition.release()
                    break
            elif evnet & select.EPOLLHUP: #链接断开
              print "hup event"
              self._CloseConnect(fileno)
          except BaseException, e:
            print "Event thread error " + str(e)
            self._CloseConnect(fileno)
    except BaseException, e:
      print str(e)
    finally:
      self.epoll.unregister(self.server.fileno())
      self.epoll.close()
      self.server.close()


class WorkThread(threading.Thread):
  def setObject(self, obj):
    self.obj = obj 

  def setServer(self, server):
    self.server = server

  """ 工作线程 """
  def run(self):
    obj = copy.deepcopy(self.obj)
    obj.setServer(self.server)
    while True:
      try:
        if self.server.condition.acquire():
          self.server.condition.wait()
          self.server.condition.release()

          while True:
            item = self.server.getFromQueue()
            if item == None:
              break

            #TODO:如果链接已经断开就不处理了
            #if item["connection"] =  
            request = cPickle.loads(item["data"])
            pargma = cPickle.loads(request["data"])

            response = {}
            response["requestid"] = request["requestid"]
            response["return"] = 0

            result = ""
            try:
              method = getattr(obj, request["fun"])
              result = method(*pargma)
              if result == None:
                continue
            except BaseException, e:
              response["return"] = -1
              response["message"] = str(e)

            response["result"] = cPickle.dumps(result)
            temp_data = cPickle.dumps(response)
            length = len(temp_data)
            finaldata = struct.pack("@I", length)
            finaldata += temp_data

            item["connection"].send(finaldata)
      except BaseException, e:
        print "work thread error " + str(e)


if __name__ == "__main__":
  #####远程调用对象,需要与client中的对象对应
  #TODO:根据这个类自动生成对应的client对象
  #类实现限制:
  #1.所有参数都为输入参数，不能做为输出,输出通过返回值获得
  #2.函数返回None表示不需要给客户端返回数据
  #3.需要实现setserver接口
  class Test():
    def setServer(self, server):
      self.server = server

    def sayHello(self, data, data2):
      data = "hello" + data + data2
      result = dict()
      result["result"] = 1
      result["data"] = data
      return result


  server = PAFServer(Test(), 3)
  server.init("127.0.0.1", 8412, socket.SOCK_STREAM)
  server.start()
