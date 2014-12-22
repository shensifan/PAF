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
TODO:   状态机不完整，STARTING和STOPING等中状态出错不能恢复
        这个服务中最好不要使用os.system,会触发SIGCHLD
        服务重启时不能自动恢复正在管理的服务,最主要的问题是不会再触发SIGCHLD
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
import shutil

import ServerManager

bcloud_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, bcloud_dir)
import Util
import PAF

class Node(PAF.PAFServer.PAFServerObj):
    """
    node server
    """
    def init(self):
        """
        初始化
        """
        return True

    def start(self, server_info, current):
        """
        启动
        """
        print "start %s" % server_info
        if type(server_info) is types.StringType:
            server_info = ServerInfo.ServerInfo(server_info)

        #启动server
        if not os.path.exists(sm.serverFile(server_info)):
            return "no this file %s" % sm.serverFile(server_info)

        (old, new) = sm.cmpAndChange(server_info, \
                                    ServerManager.ServerManager.STAT_STOP, \
                                    ServerManager.ServerManager.STAT_STARTING)
        if old == ServerManager.ServerManager.STAT_NONE:
            return "no this server %s" % server_info
        if new != ServerManager.ServerManager.STAT_STARTING:
            return "%s stat is %s" % (server_info, old)
        if old == ServerManager.ServerManager.STAT_STARTING:
            return "%s has starting" % (server_info)

        pid = os.fork()
        if pid == 0:
            #关闭从父进程继承过来的句柄
            Util.common.closeFd()
            #execl("程序文件", argv[0], argv[1], ...)
            #argv[0]相当于在ps中显示出来的名字
            os.execl(self.server.config["Private"].PYTHON, \
                    str(server_info), sm.serverFile(server_info))
            os._exit(0)
        else:
            sm.setPid(server_info, pid)
            return "OK"

    def stop(self, server_info, current):
        """
        停止
        """
        print "stop %s" % server_info
        if type(server_info) is types.StringType:
            server_info = ServerInfo.ServerInfo(server_info)
        (old, new) = sm.cmpAndChange(server_info, \
                                    ServerManager.ServerManager.STAT_RUN, \
                                    ServerManager.ServerManager.STAT_STOPING)
        if old == ServerManager.ServerManager.STAT_NONE:
            return "no this server %s" % server_info
        if new != ServerManager.ServerManager.STAT_STOPING:
            return "%s stat is %s" % (server_info, old)
        if old == ServerManager.ServerManager.STAT_STOPING:
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
            #backup_path = os.path.join(sm.backupRoot(), "%s_%s.tar.bz2" % (server_info.server, time.time()))
            #os.system("cd %s;tar -cjf %s.tar.bz2 *" % (deploy_path, backup_path))
            shutil.rmtree(deploy_path)
        Util.common.mkdirs(deploy_path)
        
        #解压服务,重新部署
        d = StringIO.StringIO()
        d.write(data)
        z = zipfile.ZipFile(d, "r")
        z.extractall(deploy_path)

        #更新状态
        (old, new) = sm.cmpAndChange(server_info, \
                                    ServerManager.ServerManager.STAT_DEPLOY, \
                                    ServerManager.ServerManager.STAT_STOP)
        if old == ServerManager.ServerManager.STAT_NONE:
            return "no this server %s" % server_info
        if new != ServerManager.ServerManager.STAT_STOP:
            return "%s stat is %s" % (server_info, old)
        if old == ServerManager.ServerManager.STAT_STOP:
            return "%s has deploy" % (server_info)
        return "OK"

    def status(self, server_info, current):
        """
        查询状态
        """
        print "status %s" % server_info
        if type(server_info) is types.StringType:
            server_info = ServerInfo.ServerInfo(server_info)
        return sm.status(server_info)

    def list(self, current):
        """
        得到全部server
        """
        print "list"
        return sm.list()

    def remove(self, server_info, current):
        """
        移除
        """
        pass

    def register(self, server_info, pid, current):
        """
        注册
        """
        if type(server_info) is types.StringType:
            server_info = ServerInfo.ServerInfo(server_info)
        #(old, new) = sm.cmpAndChange(server_info, \
        #                            ServerManager.ServerManager.STAT_STARTING, \
        #                            ServerManager.ServerManager.STAT_RUN)
        #if old == ServerManager.ServerManager.STAT_NONE:
        #    return "no this server %s" % server_info
        #if new != ServerManager.ServerManager.STAT_RUN:
        #    return "%s stat is %s" % (server_info, old)
        #if old == ServerManager.ServerManager.STAT_RUN:
        #    return "%s has RUN" % (server_info)
        return sm.heartBit(server_info, pid)

#========================================================================================================

def handler(signu, frame):
    """
    SIGCHILD
    """
    try:
        (pid, ret) = os.wait()
        sm.serverStop(pid)
    except Exception as e:
        print "wait exception %s\n" % str(e)

if __name__ == "__main__":
    signal.signal(signal.SIGCHLD, handler)

    sm = ServerManager.ServerManager()
    #恢复已经部署的服务
    sm.reload()
    sm.start()

    #启动server
    server = PAF.PAFServer.PAFServer(Node(), os.path.join(os.path.dirname(__file__), 'config.py'))
    server.start()
    sm.terminate()
