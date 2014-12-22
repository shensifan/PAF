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
import multiprocessing
sys.path.insert(0, "%s/.." % os.path.dirname(__file__))
import Util

class PAFClient():
    def __init__(self, config_file):
        try:
            self.config_file = config_file
            self.config = dict()
            execfile(config_file, self.config)

            self.requestid_prefix = str(socket.gethostbyname(socket.gethostname())) \
                + "_" + str(os.getpid()) + "_"
            self.requestid_suffix = random.randrange(0, 1000000000)
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

            self.termination = False

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
            self.response_queue = Queue.Queue(maxsize = 0)
            self.condition = threading.Condition()

            #回调线程
            self.callbacks = []
            for index in range(0, self.config["Client"].CALLBACK_COUNT):
                self.callbacks.append(CallBackThread())
                self.callbacks[index].setClient(self)
                self.callbacks[index].setDaemon(True)
                if self.config["Public"].USE_PIPE:
                    self.callbacks[index].setupPipe()
                self.callbacks[index].start()

            #启动事件处理线程
            self.event_thread = EventThread()
            self.event_thread.setClient(self)
            self.event_thread.setDaemon(True)
            self.event_thread.start()
        except BaseException as e:
            print("init error " + str(e))
            os._exit(0)

    def terminate(self):
        """
        停止
        """
        self.termination = True
        #等待所有线程停止
        for index in range(0, self.config["Client"].CALLBACK_COUNT):
            self.callbacks[index].join()
        self.event_thread.join()
        
    def getRequestId(self):
        """
        生成requestid
        格式: <host ip>_<pid>_<time>_<random number>
        <random number>初始化后新的请求会递增
        """
        if not self.requestid_lock.acquire():
            self.log.Print("require requestid lock error")
            return self.requestid_prefix + str(time.time()) + \
                "_" + str(random.randrange(0, 1000000000))

        try:
            self.requestid_suffix += 1
            return self.requestid_prefix + str(time.time()) + \
                "_" + str(self.requestid_suffix)
        except BaseException as e:
            self.log.Print("release requestid lock error")
            return self.requestid_prefix + str(time.time()) + \
                "_" + str(random.randrange(0, 1000000000))
        finally:
            self.requestid_lock.release()

    def put2Queue(self, requestid):
        """
        callback信息入队列
        """
        try:
            self.response_queue.put(requestid)
        except BaseException as e:
            self.log.Print("put2Queue " + str(e))
            return -1
        return 0

    def getFromQueue(self):
        """
        callback信息出队列
        """
        try:
            if not self.response_queue.empty():
                return self.response_queue.get()
        except BaseException as e:
            self.log.Print("getFromQueue error" + str(e))
            return None
        return None

    def addRequest(self, fileno, requestid, callback):
        """
        请求入队
        """
        if not self.request_lock.acquire():
            self.log.Print("require lock error")
            return -1

        try:
            if requestid in self.request:
                self.log.Print("request %s has exist" % requestid)
                return -1

            self.request[requestid] = dict()
            if callback is None: #同步调用,设置同步锁
                self.request[requestid]["lock"] = threading.Lock()
                if not self.request[requestid]["lock"].acquire():
                    self.log.Print("require request lock error")
                    return -1
            else:
                self.request[requestid]["lock"] = None

            self.request[requestid]["connect"] = fileno
            self.request[requestid]["time"] = int(time.time())
            self.request[requestid]["callback"] = callback

            self.request[requestid]["return"] = self.RESPONSE["E_WAIT"]
            self.request[requestid]["message"] = ""
            self.request[requestid]["result"] = None
        except BaseException as e:
            self.log.Print("addRequest " + str(e))
            return -1
        finally:
            self.request_lock.release()

        return 0

    def delRequest(self, requestid):
        """
        删除请求
        """
        if not self.request_lock.acquire():
            self.log.Print("require lock error")
            return None
            
        try:
            if requestid in self.request:
                del self.request[requestid]
            else:
                self.log.Print("request %s has gone" % requestid)
        finally:
            self.request_lock.release()

    def createProxy(self, name, endpoint):
        """ 
        创建服务代理,可能会抛出异常 
        """
        if not self.connect_lock.acquire():
            self.log.Print("require lock error")
            return None

        try:
            if name not in self.proxy:
                self.proxy[name] = {}
            if endpoint not in self.proxy[name]:
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
        """
        构造函数
        """
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
        """
        断开链接
        """
        """ 这个不要加锁,在调用它时调用方加锁 """
        self.has_connect = False

        if fileno in self.client.connections:
            del self.client.connections[fileno]
        try:
            self.client.epoll.unregister(fileno)
        except BaseException as e:
            self.client.log.Print("ServerProxy disconnect error: %s" % str(e))
            
        #释放所有request
        self.request_lock.acquire()
        try:
            for requestid in self.all_request:
                self.client.request[requestid]["return"] = self.client.RESPONSE["E_CONNECTRESET"]
                self.client.request[requestid]["message"] = " %s connect reset" % requestid
                if self.client.request[requestid]["callback"] is None: #同步调用
                    self.client.request[requestid]["lock"].release()
                else: #异步调用
                    self.client.put2Queue(requestid)
                    #通知回调线程
                    if self.client.condition.acquire():
                        self.client.condition.notify()
                        self.client.condition.release()

            #清空请求记录
            self.all_request.clear()
        except BaseException as e:
            print "disconnect error:" + str(e)
        finally:
            self.request_lock.release()

    def connect(self):
        """
        connect
        """
        """ 这个不要加锁,在调用它时调用方加锁 """
        if self.has_connect == True:
            return None

        try:
            self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.connection.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            #60内没有输据传输就开始侦测
            self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
            #以1秒为间隔发送侦测包
            self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 1)
            #连续5次侦测失败后断连
            self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 5)
            self.connection.settimeout(2)
            self.connection.connect(self.endpoint)
            self.connection.settimeout(0)
            self.connection.setblocking(0)

            self.response_buffer = ""
            self.client.connections[self.connection.fileno()] = self
            self.client.proxy[self.name][self.endpoint] = self

            self.client.epoll.register(self.connection.fileno(), \
                select.EPOLLIN | select.EPOLLHUP | select.EPOLLERR)
        except BaseException as e:
            self.has_connect = False
            #self.client.log.Print("ServerProxy connect: %s" % str(e))
            raise BaseException(e)

        self.has_connect = True

    def _addRequest(self, requestid, callback):
        """
        请求入队
        """
        #print "add request %d" % requestid
        self.request_lock.acquire()
        try:
            self.client.addRequest(self.connection.fileno(), requestid, callback)
            self.all_request[requestid] = int(time.time())
        finally:
            self.request_lock.release()

    def delRequest(self, requestid):
        """
        删除请求
        """
        #print "del request %d" % requestid
        self.request_lock.acquire()
        try:
            self.client.delRequest(requestid)
            if requestid in self.all_request:
                del self.all_request[requestid]
        finally:
            self.request_lock.release()

    def _connect(self):
        """
        connect
        """
        self.client.connect_lock.acquire()
        try:
            self.connect()
        finally:
            self.client.connect_lock.release()

    def __request(self, methodname, params):
        """
        发送请求
        """
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
        elif callback is not None:  #异步请求且不需要返回,相当于单向调用
            self._addRequest(requestid, callback)

        #加锁,保证发送数据报的完整
        if not self.send_lock.acquire():
            self.log.Print("require lock error")
            raise BaseException("require lock error")

        send_succ = True
        try:
            send_len = 0
            while send_len < len(finaldata):
                try:
                    sent = self.connection.send(finaldata[send_len:])
                    if sent <= 0:
                        Util.log.colorprint("RED", "connect has disconnected")
                        self.disconnect(self.connection.fileno())
                        send_succ = False
                        break
                    send_len += sent
                except BaseException as e:
                    if e[0] != 11: #resource temp unavaliable
                        print e
                        traceback.print_exc()
                        Util.log.colorprint("RED", "connect has disconnected")
                        self.disconnect(self.connection.fileno())
                        send_succ = False
                        break
                    time.sleep(0.1)
        finally:
            self.send_lock.release()
        
        if async: #异步调用
            return send_succ
        
        try:
            #同步调用,等待解锁
            if not self.client.request[requestid]["lock"].acquire():
                self.log.Print("require lock error")
                raise BaseException("require lock error")

            #调用出错
            if self.client.request[requestid]["return"] < 0:
                msg = self.client.request[requestid]["message"]
                raise BaseException(msg)

            #正常返回
            return self.client.request[requestid]["result"]
        finally:
            self.delRequest(requestid)

    def __getattr__(self, name):
        return _Method(self.__request, name)


class EventThread(threading.Thread):
    """ 事件处理线程 """
    def setClient(self, client):
        """
        setClient
        """
        self.client = client

    def _CloseConnect(self, fileno):
        """
        关闭链接
        """
        if not self.client.connect_lock.acquire():
            self.client.log.Print("connecto lock acquire error")
            return None
        try:
            if fileno not in self.client.connections:
                self.client.log.Print("%d has closed" % fileno)
                return
            
            #disconnect中会执行 del self.client.connections[fileno]
            #不确定会不会有问题,使用一个临时变量保证对象可用
            proxy = self.client.connections[fileno]
            proxy.disconnect(fileno)
            self.client.log.Print("%d closed" % fileno)
        finally:
            self.client.connect_lock.release()

    def run(self):
        """
        线程run
        """
        try:
            while True:
                if self.client.termination == True:
                    return 0
                try:
                    events = self.client.epoll.poll(0.5)
                except BaseException as e:
                    time.sleep(0.010)
                    continue
                for fileno, event in events:
                    try:
                        if event & select.EPOLLIN: #数据信息
                            if not self.client.connect_lock.acquire():
                                self.client.log.Print("connecto lock acquire error")
                                continue
                            try:
                                if fileno not in self.client.connections:
                                    self.client.log.Print("connect has gone")
                                    continue
                                proxy = self.client.connections[fileno]
                            finally:
                                self.client.connect_lock.release()

                            try:
                                msg = proxy.connection.recv(1024 * 1024 * 16)
                            except:
                                msg = ''

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

                                response = cPickle.loads(proxy.response_buffer[4:(length + 4)])
                                proxy.response_buffer = proxy.response_buffer[4 + length:]

                                requestid = response["requestid"]

                                #处理数据
                                if not self.client.request_lock.acquire():
                                    self.client.log.Print("request lock error")
                                    continue

                                try:
                                    if requestid not in self.client.request:
                                        self.client.log.Print("request %s has gone" % requestid)
                                        continue

                                    self.client.request[requestid]["return"] = response["return"]
                                    if response["return"] < 0:
                                        self.client.request[requestid]["message"] = \
                                            response["message"]
                                    else:
                                        self.client.request[requestid]["result"] = \
                                            response["result"]

                                    #同步调用
                                    if self.client.request[requestid]['callback'] is None:
                                        self.client.request[requestid]["lock"].release()
                                    else:
                                        self.client.put2Queue(requestid)
                                        #通知回调线程
                                        if self.client.condition.acquire():
                                            self.client.condition.notify()
                                            self.client.condition.release()
                                finally:
                                    self.client.request_lock.release()

                        if (event & select.EPOLLHUP) or (event & select.EPOLLERR): #链接断开
                            Util.log.colorprint("RED", "epoll event HUP:%d ERR:%d" \
                                % ((event & select.EPOLLHUP), (event & select.EPOLLERR)))
                            self._CloseConnect(fileno)

                    except BaseException as e:
                        self._CloseConnect(fileno)
                        traceback.print_exc()
                        self.client.log.Print("Event thread error " + str(e))
        except BaseException as e:
            self.client.log.Print("Event thread error " + str(e))
            traceback.print_exc()


class CallBackThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.sys_clnt_p = None
        self.cmd_clnt_p = None

    """ 异步回调线程 """
    def setClient(self, client):
        """
        设置客户端句柄
        """
        self.client = client

    def setupPipe(self):
        """
        建立用于os.system和subprocessing的子进程
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
        获取和os.system子进程通信的管道
        """
        return self.sys_clnt_p

    def getCmd_Pipe(self):
        """
        获取和subprocess子进程通信的管道
        """
        return self.cmd_clnt_p

    def run(self):
        try:
            while True:
                if not self.client.condition.acquire():
                    self.client.log.Print("condition acquire error")
                    continue

                self.client.condition.wait(0.5)
                self.client.condition.release()
                if self.client.termination == True:
                    if self.sys_clnt_p is not None:
                        self.sys_clnt_p.send("bcloud:exit")
                        self.cmd_clnt_p.send("bcloud:exit")
                        self.p_sys.join()
                        self.p_cmd.join()
                    return True
                        
                while True:
                    item = self.client.getFromQueue()
                    if item is None:
                        break

                    #处理数据
                    if not self.client.request_lock.acquire():
                        self.client.log.Print("request lock error")
                        continue

                    try:
                        if item not in self.client.request:
                            self.client.log.Print("this request has gone")
                            continue
                        request = self.client.request[item]
                    finally:
                        self.client.request_lock.release()

                    try:
                        #同步调用
                        if request["callback"] is None: 
                            continue

                        #异步调用
                        try:
                            request["callback"].callback((request["return"], \
                                request["message"]), request["result"])
                        except BaseException as e:
                            tb = sys.exc_info()[2]
                            while tb is not None:
                                f = tb.tb_frame
                                lineno = tb.tb_lineno
                                co = f.f_code
                                filename = co.co_filename
                                tb = tb.tb_next
                            self.client.log.Print("callback thread error %s:%d %s " \
                                % (filename, lineno, str(e)))
                        finally:
                            if not self.client.connect_lock.acquire():
                                self.client.log.Print("connecto lock acquire error")
                                return None
                            try:
                                if request["connect"] in self.client.connections:
                                    self.client.connections[request["connect"]].delRequest(item)
                            finally:
                                self.client.connect_lock.release()

                    except BaseException as e:
                        self.client.log.Print("callback thread error " + str(e))
        except BaseException as e:
            self.client.log.Print("callback thread error " + str(e))
            traceback.print_exc()

if __name__ == "__main__":
    class CallbackSayHello(object):
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
            """
            callback
            """
            print "###############################"
            if ret[0] < 0:
                print ret[1]
            else:
                print result

    client = PAFClient("config_tmplate.py")
    #createProxy 可能会抛出异常
    try:
        t = client.createProxy("test", ('127.0.0.1', 8412))
    except BaseException as e:
        print e
        exit(0)

    print "###############################"
    try:#系统默认接口
        print t.ping()
    except BaseException as e:
        print e

    print "###############################"
    try:#队列长度系统默认接口
        print t.queueLen()
    except BaseException as e:
        print e

    print "###############################"
    try:#单向调用,同步调用不能使用单向调用
        print t.async_unidirectional("unidirectional", None)
    except BaseException as e:
        print e

    print "###############################"
    try:#不存在函数
        print t.nofun(" world")
    except BaseException as e:
        print e

    print "###############################"
    try:#参数数量错误
        print t.sayHello(" world")
    except BaseException as e:
        print e

    print "###############################"
    try:#正确调用
        print t.sayHello(" world", " sync")
    except BaseException as e:
        print e

    print "###############################"
    try:#函数抛出异常调用
        t.throwException()
    except BaseException as e:
        print e

    try:#异步不存在函数调用
        t.async_nofun(" world", CallbackSayHello())
    except BaseException as e:
        print e

    try:#异步参数错误调用
        t.async_sayHello(" world", CallbackSayHello())
    except BaseException as e:
        print e

    try:#异步正确调用
        t.async_sayHello(" world", " async", CallbackSayHello())
    except BaseException as e:
        print e

    #结束所有线程,在客户端不再使用时一定要调用
    t.terminate()

    #要等待异步返回
    try:
        time.sleep(2)
    except BaseException as e:
        print str(e)
