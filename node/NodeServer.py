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

bcloud_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, bcloud_dir)
import Util
import PAF

class ServerInfo(object):
    def __init__(self):
        self.namespace = ""
        self.application = ""
        self.server = ""

class ServerManager(object):
    def __init__(self):
        #self.server[server_info]["state"]
        #self.server[server_info]["time_start"]
        #self.server[server_info]["time_stop"]
        #self.server[server_info]["time_heart"]
        #self.server[server_info]["pid"]
        self.server = dict()
        self.lock = threading.lock()


class Node(PAF.PAFServer.PAFServerObj):
    def init(self):
        return True

    def start(self, server_info, current):
        pass

    def stop(self, server_info, current):
        pass

    def deploy(self, server_info, address, current):
        """
        server_info["namespace"]
        server_info["application"]
        server_info["server"]
        """
        pass

    def remove(self, server_info, current):
        pass
    
    ####################################

    def register(self, current):
        pass

    def heart(self, current):
        pass

#========================================================================================================

#启动server
server = PAF.PAFServer.PAFServer(Node(), 'ServerConfig.py')
server.init(config.LISTEN_IP, config.LISTEN_PORT)
server.setupPipe()
server.start()
