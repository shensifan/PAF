#!/usr/bin/env python
# -*- coding: utf-8 -*-  

################################################################################
#
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
description:结点服务功能如下:
            1.部署/移除服务
                服务部署结构
                some/path/namespace/application/server
                server中目录:
                    server PAF Util ...
            2.启动与停止服务
            3.接入本机部署服务心跳
            4.向register注册服务
Authors: shenweizheng(shenweizheng@baidu.com)
Date:    2014/11/18 17:23:06
"""
import os
import sys
import traceback
import pprint
import time
import threading
import signal
import config

bcloud_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, bcloud_dir)
import Util
import PAF

class ServerInfo(object):
    def __init__(self, namespace, application = None, server = None):
        if application is None or server is None:
            column = re.split("\.", namespace)
            if len(column) != 3:
                raise Exception("bad name")
            self.namespace = column[0]
            self.application = column[1]
            self.server = column[2]
            return

        self.namespace = namespace
        self.application = application
        self.server = server

    def __str__(self):
        return ("%s.%s.%s") % (self.namespace, self.application, self.server)

def handler(signu, frame):
    try:
        (pid, ret) = os.wait()
        sm.serverStop(pid)
    except Exception as e:
        print "wait exception %s\n" % str(e)


class ServerManager(object):
    STAT_NONE = -1      #服务不在管理中
    STAT_STOP = 0       #服务停止
    STAT_STARTING = 1   #服务正在启动
    STAT_RUN = 2        #服务正在运行
    STAT_STOPING = 3    #服务正在停止

    def __init__(self):
        #self.server[server_info]["state"]
        #self.server[server_info]["time_start"]
        #self.server[server_info]["time_stop"]
        #self.server[server_info]["time_heart"]
        #self.server[server_info]["pid"]
        #self.server[server_info]["path"]
        self.server = dict()
        self.pids = dict()
        self.lock = threading.lock()

        signal.signal(signal.SIGCHLD, handler)

        #所有服务的部署目录
        self.server_root = os.path.join(os.getcwd(), "services")
        Util.common.mkdirs(self.server_root)

    def deploy(self, server_info):
        self.lock.acquire()
        try:
            if server_info in self.server and self.server[server_info]["state"] != STAT_STOP:
                print "server stat is %s" % self.server[server_info]["state"]
                return False

            self.server[server_info]["state"] = STAT_STOP
            self.server[server_info]["time_start"] = None
            self.server[server_info]["time_stop"] = None
            self.server[server_info]["time_heart"] = None
            self.server[server_info]["pid"] = -1
            self.server[server_info]["path"] = os.path.join(self.server_root, \
                                                            server_info.namespace, \
                                                            server_info.application, \
                                                            server_info.server, \
                                                            server_info.server, "server.py")
        finally:
            self.lock.release()
        
        return STAT_NONE

    def state(self, server_info):
        self.lock.acquire()
        try:
            if server_info in self.server:
                return self.server[server_info]["state"]
            else:
                return STAT_NONE
        finally:
            self.lock.release()
        
        return STAT_NONE

    def cmpAndChange(self, server_info, old_state, new_state):
        """
        比较并更新状态,返回(旧状态，新状态)
        """
        self.lock.acquire()
        try:
            if server_info not in self.server:
                return (STAT_NONE, STAT_NONE)
            
            if self.server[server_info]["state"] == old_state:
                self.server[server_info]["state"] = new_state
                return (old_state, new_state)

            return (self.server[server_info]["state"], self.server[server_info]["state"])
        finally:
            self.lock.release()
        
    def serverFile(self, server_info):
        self.lock.acquire()
        try:
            if server_info not in self.server:
                return None
            
            return self.server[server_info]["path"]
        finally:
            self.lock.release()

    def serverStop(self, pid):
        self.lock.acquire()
        try:
            if pid not in self.pids:
                print "no this pid"
                return False

            server_info = self.pids[pid]
            if server_info not in self.server:
                print "no this server info"
                return None
            
            self.server[server_info]["state"] = ServerManager.STAT_STOP
            self.server[server_info]["time_stop"] = int(time.time())
            self.server[server_info]["pid"] = -1
            del self.pids[pid]
        finally:
            self.lock.release()

    def setPid(self, server_info, pid):
        self.lock.acquire()
        try:
            if server_info not in self.server:
                return False
            
            self.server[server_info]["pid"] = pid
            self.server[server_info]["time_start"] = int(time.time())
            self.pids[pid] = server_info
        finally:
            self.lock.release()

        return True

    def heartBit(self, server_info):
        self.lock.acquire()
        try:
            if server_info not in self.server:
                return False
            
            self.server[server_info]["time_heart"] = int(time.time())
        finally:
            self.lock.release()

        return True


class Node(PAF.PAFServer.PAFServerObj):
    def init(self):
        return True

    def start(self, server_info, current):
        (old, new) = sm.cmpAndChange(server_info, ServerManager.STAT_STOP, ServerManager.STAT_STARTING)
        if old == ServerManager.STAT_NONE:
            return "no this server %s" % server_info
        if new != ServerManager.STAT_STARTING:
            return "%s stat is %d" % (server_info, old)
        if old == ServerManager.STAT_STARTING:
            return "%s has starting" % (server_info)

        #启动server
        pid = os.fork()
        if pid == 0:
            #execl("程序文件", argv[0], argv[1], ...)
            #argv[0]相当于在ps中显示出来的名字
            os.execl(sm.serverFile(server_info), str(server_info))
            os._exit(0)
        else:
            sm.setPid(server_info, pid)
            return "OK"

    def stop(self, server_info, current):
        (old, new) = sm.cmpAndChange(server_info, ServerManager.STAT_RUN, ServerManager.STAT_STOPING)
        if old == ServerManager.STAT_NONE:
            return "no this server %s" % server_info
        if new != ServerManager.STAT_STOPING:
            return "%s stat is %d" % (server_info, old)
        if old == ServerManager.STAT_STOPING:
            return "%s has stoping" % (server_info)
        os.kill(self.server[server_info]["pid"], signal.SIGINT)

    def deploy(self, server_info, address, current):
        pass

    def remove(self, server_info, current):
        pass
    
    ####################################

    def register(self, server_info, current):
        (old, new) = sm.cmpAndChange(server_info, ServerManager.STAT_STARTING, ServerManager.STAT_RUN)
        if old == ServerManager.STAT_NONE:
            return "no this server %s" % server_info
        if new != ServerManager.STAT_RUN:
            return "%s stat is %d" % (server_info, old)
        if old == ServerManager.STAT_RUN:
            return "%s has RUN" % (server_info)

        sm.heartBit(server_info)


#========================================================================================================

#启动server
server = PAF.PAFServer.PAFServer(Node(), 'config.py')
server.init(config.LISTEN_IP, config.LISTEN_PORT)
server.setupPipe()
server.start()
