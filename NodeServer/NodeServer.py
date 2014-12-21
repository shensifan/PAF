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
TODO:   状态机不完整，STARTING和STOP等中状态出错不能恢复
"""
import os
import sys
import traceback
import pprint
import time
import threading
import signal
import types
import ServerInfo
import StringIO
import zipfile

bcloud_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, bcloud_dir)
import Util
import PAF

def SYSTEM(cmd):
    print cmd
    os.system(cmd)

def handler(signu, frame):
    try:
        (pid, ret) = os.wait()
        sm.serverStop(pid)
    except Exception as e:
        print "wait exception %s\n" % str(e)


class ServerManager(object):
    STAT_NONE = "NONE"      #服务不在管理中
    STAT_DEPLOY = "DEPLOY"     #服务正在部署
    STAT_STOP = "STOP"       #服务停止
    STAT_STARTING = "STARTING"   #服务正在启动
    STAT_RUN = "RUN"        #服务正在运行
    STAT_STOPING = "STOPING"    #服务正在停止

    def __init__(self):
        #self.server[server_info]["state"]
        #self.server[server_info]["time_start"]
        #self.server[server_info]["time_stop"]
        #self.server[server_info]["time_heart"]
        #self.server[server_info]["pid"]
        #self.server[server_info]["path"]
        self.server = dict()
        self.pids = dict()
        self.stoping = dict() #保存已经发送kill信号的程序
        self.lock = threading.Lock()

        signal.signal(signal.SIGCHLD, handler)

        #所有服务的部署目录
        self.server_root = os.path.join(os.getcwd(), "services")
        Util.common.mkdirs(self.server_root)
        self.backup_root = os.path.join(os.getcwd(), "backup")
        Util.common.mkdirs(self.backup_root)

    def deploy(self, server_info):
        self.lock.acquire()
        try:
            if server_info in self.server and self.server[server_info]["state"] != self.STAT_STOP:
                print "server stat is %s" % self.server[server_info]["state"]
                return False

            self.server[server_info] = dict()
            self.server[server_info]["state"] = self.STAT_DEPLOY
            self.server[server_info]["time_start"] = None
            self.server[server_info]["time_stop"] = None
            self.server[server_info]["time_heart"] = None
            self.server[server_info]["pid"] = -1
            self.server[server_info]["path"] = os.path.join(self.server_root, \
                                                            server_info.namespace, \
                                                            server_info.application, \
                                                            server_info.server, \
                                                            server_info.server, \
                                                            server_info.server + ".py")
        finally:
            self.lock.release()
        
        return True

    def status(self, server_info):
        self.lock.acquire()
        try:
            if server_info in self.server:
                return self.server[server_info]
            status = dict()
            status["state"] = self.STAT_NONE
            status["time_start"] = 0
            status["time_stop"] = 0
            status["time_heart"] = 0
            status["pid"] = -1
            status["path"] = ""
            return status
        finally:
            self.lock.release()
    
    def list(self):
        self.lock.acquire()
        try:
            return self.server
        finally:
            self.lock.release()

    def cmpAndChange(self, server_info, old_state, new_state):
        """
        比较并更新状态,返回(旧状态，新状态)
        """
        self.lock.acquire()
        try:
            if server_info not in self.server:
                return (self.STAT_NONE, self.STAT_NONE)
            
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

    def deployPath(self, server_info):
        return os.path.join(self.server_root, \
                            server_info.namespace, \
                            server_info.application, \
                            server_info.server)

    def backupRoot(self):
        return self.backup_root

    def stop(self, server_info, PAFServer, current):
        self.lock.acquire()
        try:
            self.stoping[server_info] = dict()
            self.stoping[server_info]["server"] = PAFServer
            self.stoping[server_info]["current"] = current
        finally:
            self.lock.release()

    def serverStop(self, pid):
        self.lock.acquire()
        try:
            if pid not in self.pids:
                return False

            server_info = self.pids[pid]
            if server_info not in self.server:
                print "no this server info"
                return False
            
            self.server[server_info]["state"] = self.STAT_STOP
            self.server[server_info]["time_stop"] = int(time.time())
            self.server[server_info]["pid"] = -1

            if server_info in self.stoping:
                s = self.stoping[server_info]["server"]
                c = self.stoping[server_info]["current"]
                s.addResponse(c["connection"], c["requestid"], 0, "", "OK")
                del self.stoping[server_info]
            del self.pids[pid]
        finally:
            self.lock.release()

        return True

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
        print "start %s" % server_info
        if type(server_info) is types.StringType:
            server_info = ServerInfo.ServerInfo(server_info)

        #启动server
        if not os.path.exists(sm.serverFile(server_info)):
            return "no this file %s" % sm.serverFile(server_info)

        (old, new) = sm.cmpAndChange(server_info, ServerManager.STAT_STOP, ServerManager.STAT_STARTING)
        if old == ServerManager.STAT_NONE:
            return "no this server %s" % server_info
        if new != ServerManager.STAT_STARTING:
            return "%s stat is %s" % (server_info, old)
        if old == ServerManager.STAT_STARTING:
            return "%s has starting" % (server_info)

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
        print "stop %s" % server_info
        if type(server_info) is types.StringType:
            server_info = ServerInfo.ServerInfo(server_info)
        (old, new) = sm.cmpAndChange(server_info, ServerManager.STAT_RUN, ServerManager.STAT_STOPING)
        if old == ServerManager.STAT_NONE:
            return "no this server %s" % server_info
        if new != ServerManager.STAT_STOPING:
            return "%s stat is %s" % (server_info, old)
        if old == ServerManager.STAT_STOPING:
            return "%s has stoping" % (server_info)
        os.kill(sm.server[server_info]["pid"], 9)
        sm.stop(server_info, self.server, current)
        return None

    def deploy(self, server_info, data, current):
        """
        部署服务
        TODO:需要校验部署位置是否与config中写的一样
        """
        if type(server_info) is types.StringType:
            server_info = ServerInfo.ServerInfo(server_info)
        if not sm.deploy(server_info):
            return "False"

        #删除代码
        deploy_path = sm.deployPath(server_info)
        if os.path.exists(deploy_path):
            backup_path = os.path.join(sm.backupRoot(), "%s_%s.tar.bz2" % (server_info.server, time.time()))
            os.system("cd %s;tar -cjf %s.tar.bz2 *" % (deploy_path, backup_path))
            os.system("rm -rf %s/*" % deploy_path)
        else:
            Util.common.mkdirs(deploy_path)
        
        #解压服务,重新部署
        d = StringIO.StringIO()
        d.write(data)
        z = zipfile.ZipFile(d, "r")
        z.extractall(deploy_path)

        #更新状态
        (old, new) = sm.cmpAndChange(server_info, ServerManager.STAT_DEPLOY, ServerManager.STAT_STOP)
        if old == ServerManager.STAT_NONE:
            return "no this server %s" % server_info
        if new != ServerManager.STAT_STOP:
            return "%s stat is %s" % (server_info, old)
        if old == ServerManager.STAT_STOP:
            return "%s has deploy" % (server_info)
        return "OK"

    def status(self, server_info, current):
        print "status %s" % server_info
        if type(server_info) is types.StringType:
            server_info = ServerInfo.ServerInfo(server_info)
        return sm.status(server_info)

    def list(self, current):
        print "list"
        return sm.list()

    def remove(self, server_info, current):
        pass
    
    ####################################

    def register(self, server_info, current):
        if type(server_info) is types.StringType:
            server_info = ServerInfo.ServerInfo(server_info)
        (old, new) = sm.cmpAndChange(server_info, ServerManager.STAT_STARTING, ServerManager.STAT_RUN)
        if old == ServerManager.STAT_NONE:
            return "no this server %s" % server_info
        if new != ServerManager.STAT_RUN:
            return "%s stat is %s" % (server_info, old)
        if old == ServerManager.STAT_RUN:
            return "%s has RUN" % (server_info)

        sm.heartBit(server_info)


#========================================================================================================

if __name__ == "__main__":
    sm = ServerManager()
    #启动server
    server = PAF.PAFServer.PAFServer(Node(), 'config.py')
    server.start()
