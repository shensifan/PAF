#!/usr/bin/env python
# -*- coding: utf-8 -*-
################################################################################
#
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
	Description :
	Authors     : shenweizheng(shenweizheng@baidu.com)
	Date        : 2014-12-22 11:43:20
"""
import os
import sys
import traceback
import pprint
import time
import threading
import ServerInfo

bcloud_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, bcloud_dir)
import Util

class ServerManager(threading.Thread):
    """
    服务管理类
    """
    STAT_NONE = "NONE"      #服务不在管理中
    STAT_DEPLOY = "DEPLOY"     #服务正在部署
    STAT_STOP = "STOP"       #服务停止
    STAT_STARTING = "STARTING"   #服务正在启动
    STAT_RUN = "RUN"        #服务正在运行
    STAT_STOPING = "STOPING"    #服务正在停止

    def __init__(self):
        threading.Thread.__init__(self)

        #self.server[server_info]["state"]
        #self.server[server_info]["time_start"]
        #self.server[server_info]["time_stop"]
        #self.server[server_info]["time_heart"]
        #self.server[server_info]["pid"]
        #self.server[server_info]["path"]
        self.server = dict()

        #self.pids[pid] = server_info
        self.pids = dict()

        #self.stoping[server_info]["pid"] = pid
        #self.stoping[server_info]["server"] = PAFServer
        #self.stoping[server_info]["current"] = current
        self.stoping = dict() #保存已经发送kill信号的程序
        self.stoping_nochild = dict() #保存已经发送kill信号的程序,不是自己启动的

        self.lock = threading.Lock()

        #所有服务的部署目录
        self.server_root = os.path.join(os.getcwd(), "services")
        Util.common.mkdirs(self.server_root)
        self.backup_root = os.path.join(os.getcwd(), "backup")
        Util.common.mkdirs(self.backup_root)

        self.termination = False

    def deploy(self, server_info):
        """
        部署
        """
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
        """
        状态查询
        """
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
        """
        返回所有server信息
        """
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
        """
        返回服务执行文件
        """
        self.lock.acquire()
        try:
            if server_info not in self.server:
                return None
            
            return self.server[server_info]["path"]
        finally:
            self.lock.release()

    def deployPath(self, server_info):
        """
        得到部署目录
        """
        return os.path.join(self.server_root, \
                            server_info.namespace, \
                            server_info.application, \
                            server_info.server)

    def backupRoot(self):
        """
        得到备份目录
        """
        return self.backup_root

    def stop(self, server_info, PAFServer, current):
        """
        停止
        """
        self.lock.acquire()
        try:
            if self.server[server_info]["pid"] in self.pids:
                print "add to stoping"
                self.stoping[server_info] = dict()
                self.stoping[server_info]["pid"] = self.server[server_info]["pid"]
                self.stoping[server_info]["server"] = PAFServer
                self.stoping[server_info]["current"] = current
            else:
                print "add to stoping_nochild"
                self.stoping_nochild[server_info] = dict()
                self.stoping_nochild[server_info]["pid"] = self.server[server_info]["pid"]
                self.stoping_nochild[server_info]["server"] = PAFServer
                self.stoping_nochild[server_info]["current"] = current
        finally:
            self.lock.release()

    def serverStop(self, pid):
        """
        停止
        """
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

    def serverStopNoChild(self, server_info):
        """
        调用方已经加锁
        """
        if server_info not in self.server:
            print "no this server info"
            return False
        
        self.server[server_info]["state"] = self.STAT_STOP
        self.server[server_info]["time_stop"] = int(time.time())
        self.server[server_info]["pid"] = -1

        if server_info in self.stoping_nochild:
            s = self.stoping_nochild[server_info]["server"]
            c = self.stoping_nochild[server_info]["current"]
            s.addResponse(c["connection"], c["requestid"], 0, "", "OK")
            del self.stoping_nochild[server_info]

        return True

    def setPid(self, server_info, pid):
        """
        设置进程ID
        """
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

    def heartBit(self, server_info, pid):
        """
        心跳
        """
        self.lock.acquire()
        try:
            if server_info not in self.server:
                return False
            
            self.server[server_info]["state"] = self.STAT_RUN
            self.server[server_info]["pid"] = pid
            self.server[server_info]["time_heart"] = int(time.time())
        finally:
            self.lock.release()

        return True

    def terminate(self):
        """
        结束
        """
        self.termination = True

    def reload(self):
        """
        重新加载已经部署服务
        """
        for namespace in os.listdir(self.server_root):
            for application in os.listdir(os.path.join(self.server_root, namespace)):
                for service in os.listdir(os.path.join(self.server_root, namespace, application)):
                    server_info = ServerInfo.ServerInfo(namespace, \
                                                        application, \
                                                        service)
                    self.deploy(server_info)
                    self.cmpAndChange(server_info, self.STAT_DEPLOY, self.STAT_STOP)

    def run(self):
        while self.termination == False:
            time.sleep(1)
            try:
                self.lock.acquire()
                try:
                    for item in self.stoping_nochild.keys():
                        print item
                        if os.path.exists("/proc/%s" % self.stoping_nochild[item]["pid"]):
                            continue
                        print "stop %s" % item
                        self.serverStopNoChild(item)
                finally:
                    self.lock.release()
            except Exception as e:
                print str(e)
