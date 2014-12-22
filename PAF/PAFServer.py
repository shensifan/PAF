#!/usr/bin/env python
# -*- coding: utf-8 -*-  
#TODO:目前只处理TCP
import os
import sys
import socket
import select
import Queue
import thread
import time
import json
import threading
import struct
import cPickle
import copy
import PAFClient
import traceback
import multiprocessing
sys.path.insert(0, "%s/.." % os.path.dirname(__file__))
import Util

class PAFServer():
    def __init__(self, obj, config_file):
        try:
            self.log = Util.Log(prefix = "PAFServer")

            self.config_file = config_file
            self.config = dict()
            execfile(config_file, dict(), self.config)

            self.epoll = select.epoll()

            #response返回错误码
            self.RESPONSE = dict()
            self.RESPONSE["E_OK"] = 0
            self.RESPONSE["E_CLOSE"] = -1 #标识需要断开链接
            self.RESPONSE["E_UNKNOWN"] = -99  #不可知错误

            #监听server
            self.server = None

            #PAF客户端用于服务中调用其它PAF服务
            self.client = None

            #所有链接
            self.connections = dict()
            self.requests_buffer = dict()

            #服务对象
            self.obj = obj

            #主线程与工作线程通信队列
            self.request_queue = Queue.Queue(maxsize = 0)
            self.request_condition = threading.Condition()

            #工作线与主线程程通信队列
            self.response_queue = Queue.Queue(maxsize = 0)
            self.response_condition = threading.Condition()

            #是否退出
            self.termination = False

            #工作线程
            self.workers = list()
            #回复线程
            self.resp = None
            
            #基础服务
            #self.log_server = None
            self.node_server = None
            #self.stat_server = None
        except BaseException as e:
            print("init error " + str(e))
            os._exit(0)

    def setConfig(self, conf, name, value):
        if conf not in self.config:
            self.config[conf] = dict()
        self.config[conf].name = value
        return 0

    def getConfig(self, conf):
        if conf not in self.config:
            return None

        return self.config[conf]

    def _setupPipe(self):
        """
        为工作线程建立用于执行os.system,subprocess的轻量级进程
        """
        try:
            for index in range(0, self.config["Server"].WORKER_COUNT):
                self.workers[index].setupPipe()
        except BaseException as e:
            self.log.Print("set Pipe error " + str(e))
            return -1

        return 0


    def _initServer(self):
        try:
            #工作线程
            for index in range(0, self.config["Server"].WORKER_COUNT):
                self.workers.append(WorkThread(self.obj, self, self.config))
                self.workers[index].setDaemon(True)
            if self.config["Public"].USE_PIPE and self._setupPipe() < 0: 
                return -1

            #回复线程
            self.resp = RespThread()
            self.resp.setServer(self)
            self.resp.setDaemon(True)
            
            #监听服务
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server.bind((self.config["Server"].LISTEN_IP, self.config["Server"].LISTEN_PORT))
            self.server.listen(10000)
            self.server.setblocking(0)
            self.epoll.register(self.server.fileno(), \
                select.EPOLLIN | select.EPOLLHUP | select.EPOLLERR)
        except Exception as e:
            self.log.Print("init server error " + str(e))
            traceback.print_exc()
            return -1

        return 0

    def _initClient(self):
        try:
            #客户端,供服务对象调用其它服务使用
            self.client = PAFClient.PAFClient(self.config_file)
        except BaseException as e:
            self.log.Print("init client error " + str(e))
            return -1

        return 0

    def _heartBeat(self):
        if self.node_server is None:
            return 0

        item = "%s.%s.%s" % (self.config["Server"].NAMESPACE, \
                    self.config["Server"].APPLICATION, \
                    self.config["Server"].SERVICE)
        try:
            self.node_server.register(item, os.getpid())
        except BaseException as e:
            self.log.Print("register error " + str(e))
            return -1

        return 0

    def start(self):
        """
        启动服务
        """
        if self._initClient() < 0:
            return -1

        if self._initServer() < 0:
            return -1

#        try:
#            if hasattr(self.config["Server"], "LOG_SERVER" ) and self.config["Server"].LOG_SERVER != None:
#                self.log_server = self.client.createProxy('LOGSERVER', self.config["Server"].LOG_SERVER)
#        except BaseException as e:
#            self.log.Print("no connect to LogServer")

        try:
            if hasattr(self.config["Server"], "NODE_SERVER") and self.config["Server"].NODE_SERVER != None:
                self.node_server = self.client.createProxy('NODESERVER', self.config["Server"].NODE_SERVER)
        except BaseException as e:
            traceback.print_exc()
            self.log.Print("no connect to NodeServer %s" % str(e))

        #启动工作/回复线程
        try:
            for index in range(0, self.config["Server"].WORKER_COUNT):
                self.workers[index].start()
            self.resp.start()
        except BaseException as e:
            self.log.Print("start work thread error " + str(e))
            return -1
        
        time_heart = 0
        while True:
            if int(time.time()) - time_heart > 1:
                self._heartBeat()
                time_heart = int(time.time())

            if self.termination == True:
                for index in range(0, self.config["Server"].WORKER_COUNT):
                    self.workers[index].join()
                self.resp.join()
                self.client.terminate()
                break

            try:
                events = self.epoll.poll(0.5)
            except KeyboardInterrupt:
                self.termination = True
                continue
            except BaseException as e:
                self.log.Print("epoll error " + str(e))
                traceback.print_exc()
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
                        if self.request_condition.acquire():
                            self.request_condition.notifyAll()
                            self.request_condition.release()
                    except BaseException as e:
                        self.log.Print("notify to work thread error " + str(e))

                if (event & select.EPOLLHUP) or (event & select.EPOLLERR): #链接断开
                    Util.log.colorprint("RED", "epoll event HUP:%d ERR:%d" \
                        % ((event & select.EPOLLHUP), (event & select.EPOLLERR)))
                    self._CloseConnect(fileno)

    def addRequest(self, fileno, data):
        """
        加请求进队列
        """
        try:
            request = cPickle.loads(data)
        except BaseException as e:
            self._CloseConnect(fileno)
            self.log.Print("loads data error " + str(e))
            return -1

        item = dict()
        item["connection"] = fileno
        item["requestid"] = request["requestid"]
        item["fun"] = request["fun"]
        current = dict()
        current.update(item)
        item["pargma"] = list(request["pargma"])
        item["pargma"].append(current)
        item["in_queue_time"] = time.time()
        try:
            self.request_queue.put(item)
        except BaseException as e:
            self._CloseConnect(fileno)
            self.log.Print("addRequest error " + str(e))
            return -1
        return 0

    def getRequest(self):
        """
        取请求
        """
        try:
            if not self.request_queue.empty():
                item = self.request_queue.get()
                item["out_queue_time"] = time.time()
                return item
        except BaseException as e:
            self.log.Print("error" + str(e))
            return None
        return None

    def addResponse(self, fileno, requestid, ret, message, result):
        """
        加返回数据进队列
        """
        item = dict()
        item["requestid"] = requestid
        item["return"] = copy.deepcopy(ret)
        item["message"] = message
        item["connection"] = fileno
        item["result"] = copy.deepcopy(result)
        try:
            self.response_queue.put(item)
        except BaseException as e:
            self.log.Print("error" + str(e))
            return -1
        try:
            if self.response_condition.acquire():
                self.response_condition.notifyAll()
                self.response_condition.release()
        except BaseException as e:
            self.log.Print("notify to work thread error " + str(e))

        return 0

    def getResponse(self):
        """
        从队列获取返回数据
        """
        try:
            if not self.response_queue.empty():
                return self.response_queue.get()
        except BaseException as e:
            self.log.Print("error" + str(e))
            return None
        return None

    def _CloseConnect(self, fileno):
        """
        关闭链接
        """
        try:
            self.epoll.unregister(fileno)
            self.connections[fileno].close()
            del self.connections[fileno]
            del self.requests_buffer[fileno]
            self.log.Print("%d closed" % fileno)
        except BaseException as e:
            self.log.Print("close connect %d error %s" % (fileno, str(e)))

    def _accept(self):
        """
        accept
        """
        try:
            connection, address = self.server.accept()
            connection.setblocking(0)
            self.epoll.register(connection.fileno(), \
                select.EPOLLIN | select.EPOLLHUP | select.EPOLLERR)
            self.connections[connection.fileno()] = connection
            self.requests_buffer[connection.fileno()] = ''
        except BaseException as e:
            self.log.Print("accept error " + str(e))

    def _recv(self, fileno):
        """
        接收数据并解析
        """
        try:
            msg = self.connections[fileno].recv(1024 * 1024 * 16)
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
                if self.addRequest(fileno, self.requests_buffer[fileno][4:(length + 4)]) == 0:
                    self.requests_buffer[fileno] = self.requests_buffer[fileno][4 + length:]
        except BaseException as e:
            self.log.Print("recv error %s" % str(e))
            self._CloseConnect(fileno)
            return -1

        return 0


class WorkThread(threading.Thread):
    """
    工作线程
    """
    def __init__(self, obj, server, config):
        threading.Thread.__init__(self)
        self.obj = obj
        self.sys_clnt_p = None
        self.cmd_clnt_p = None
        self.server = server
        self.config = config

    def setupPipe(self):
        """
        为工作线程建立用于执行os.system,subprocess的轻量级进程
        """
        sys_server_p, sys_client_p = multiprocessing.Pipe()
        cmd_server_p, cmd_client_p = multiprocessing.Pipe()
        self.p_sys = multiprocessing.Process(target=Util.proccall.sys_call_proc, \
            args=(sys_server_p,))
        self.p_cmd = multiprocessing.Process(target=Util.proccall.runCommand_proc, \
            args=(cmd_server_p,))
        self.p_sys.start()
        self.p_cmd.start()
        self.sys_clnt_p = sys_client_p
        self.cmd_clnt_p = cmd_client_p

    def getSys_Pipe(self):
        """
        获取用于执行os.system命令的管道
        """
        return self.sys_clnt_p

    def getCmd_Pipe(self):
        """
        获取用于执行subprocess的管道
        """
        return self.cmd_clnt_p

    """ 工作线程 """
    def run(self):
        obj = copy.deepcopy(self.obj)
        obj.setServer(self.server)

        #如果存在init函数，调用
        try:
            if not obj.init():
                self.server.log.Print("obj init error")
                exit(0)
        except BaseException as e:
            pass

        while not self.server.termination:
            if not self.server.request_condition.acquire():
                Util.log.colorprint("RED", "self.server.request_condition.acquire fail")
                continue

            self.server.request_condition.wait(0.1)
            self.server.request_condition.release()

            while True:
                item = self.server.getRequest()
                if item is None:
                    break

                try:
                    method = getattr(obj, item["fun"])
                except BaseException as e:
                    self.server.log.Print("error when set method for " \
                        + item['fun'] + ': ' + str(e))
                    self.server.addResponse(item["connection"], item["requestid"], \
                        self.server.RESPONSE["E_UNKNOWN"], str(e), "")
                    continue

                try:
                    #temp_line_no = sys._getframe().f_lineno + 1  #!!!!得到下一行行号,与下一行之前不能有空行
                    result = method(*item["pargma"])
                except BaseException as e:
                    tb = sys.exc_info()[2]
                    while tb is not None:
                        f = tb.tb_frame
                        lineno = tb.tb_lineno
                        co = f.f_code
                        filename = co.co_filename
                        tb = tb.tb_next

                    self.server.log.Print("error when call method for " \
                        + item['fun'] + ': ' + str(e))
                    traceback.print_exc()
                    self.server.addResponse(item["connection"], item["requestid"], \
                        self.server.RESPONSE["E_UNKNOWN"], "%s:%d %s" % \
                            (filename, lineno, str(e)), "")
                    continue

                #函数返回None表示不用给客户端回复
                if result is None:
                    continue

                self.server.addResponse(item["connection"], item["requestid"], \
                    self.server.RESPONSE["E_OK"], "", result)

        if self.sys_clnt_p is not None:
            self.sys_clnt_p.send("bcloud:exit")
            self.cmd_clnt_p.send("bcloud:exit")
            self.p_sys.join()
            self.p_cmd.join()

class RespThread(threading.Thread):
    def setServer(self, server):
        """
        设置接受请求的服务器对象
        """
        self.server = server
        self.last_print_time = time.time()

    def run(self):
        """ 工作线程 """
        while not self.server.termination:
            if not self.server.response_condition.acquire():
                Util.log.colorprint("RED", "self.server.response_condition.acquire fail")
                continue
            self.server.response_condition.wait(0.5)
            self.server.response_condition.release()
    
            while True:
                if time.time() - self.last_print_time > 60:
                    Util.log.colorprint("DEFAULT", "RespThread: \
                        PAFServer request q len: %d, response q len: %d" % \
                        (self.server.request_queue.qsize(), self.server.response_queue.qsize()))
                    self.last_print_time = time.time()

                item = self.server.getResponse()
                if item is None:
                    break
    
                if item["return"] == self.server.RESPONSE["E_CLOSE"]:
                    self.server._CloseConnect(item["connection"])
    
                if item["connection"] not in self.server.connections:
                    self.server.log.Print("%d connect has gone" % item["connection"])
                    continue
    
                temp_data = cPickle.dumps(item)
                length = len(temp_data)
                finaldata = struct.pack("@I", length)
                finaldata += temp_data
                send_len = 0
                while send_len < len(finaldata):
                    try:
                        sent = self.server.connections[item["connection"]] \
                            .send(finaldata[send_len:])
                        if sent <= 0:
                            self.server.log.Print("connect has disconnected")
                            self.server._CloseConnect(item["connection"])
                            break
                        send_len += sent 
                    except BaseException as e:
                        if e[0] != 11: #resource temp unavaliable
                            self.server.log.Print("connect has disconnected" + str(e))
                            self.server._CloseConnect(item["connection"])
                            break
                        time.sleep(0.01)


class PAFServerObj(object):
    """
    服务基类，所有服务类从此类继承
    """
    def setServer(self, server):
        """
        setServer
        """
        self.server = server

    def ping(self, current):
        """
        服务存活接口
        """
        return "pong"

    def queueLen(self, current):
        """
        获得队列长度
        """
        msg = dict()
        msg["request"] = self.server.request_queue.qsize()
        msg["response"] = self.server.response_queue.qsize()
        return msg

    def setConfig(self, conf, name, value, current):
        """
        更新配置,目前只允许更新日志级别和私有配置 
        TODO:因为self.conf为全局变量会影响其它线程且存在线程安全问题
        """
        if conf not in self.server.config:
            self.server.config[conf] = dict()
        if conf == "Private":
            self.server.config[conf].name = value
            return "OK"
        if conf == "Server" and name == "LOG_LEVEL":
            self.server.config[conf].name = value
            return "OK"

        return "faild not valid %s %s %s" % (conf, name, value)


if __name__ == "__main__":
    #类实现限制:
    #1.所有参数都为输入参数，不能做为输出,输出通过返回值获得
    #2.函数返回None表示不需要给客户端返回数据
    #3.都必须继承自PAFServerObj
    #4.所有对外提供的接口需要以current参数为最后一个参数,这个参数为系统使用,不是接口参数
    class Test(PAFServerObj):
        """
        测试服务对象
        """
        def sayHello(self, data, data2, current):
            """
            sayHello
            """
            data = "%s hello" % current["requestid"] + data + data2
            result = dict()
            result["result"] = 1
            result["data"] = data
            return result

        def throwException(self, current):
            """
            exception
            """
            raise BaseException("%s a exception" % current["requestid"])

        def unidirectional(self, data, current):
            """
            unidirectional
            """
            print "unidirectional call " + str(data)


    server = PAFServer(Test(), "config_tmplate.py")
    server.start()
