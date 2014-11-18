#!/usr/bin/env python
# -*- coding: utf-8 -*-  

################################################################################
#
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
This module is the Node server.

Authors: shenweizheng(shenweizheng@baidu.com)
Date:    2014/11/18 17:23:06
"""

import os
import sys
import traceback
import pprint
import time

import common
import config
import createtarget
import taskmanager
import distaskmanager
import sourcemanager
import utmanager
import heart
import socket
import threading

bcloud_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, bcloud_dir)
import Util
import PAF

class Node(PAF.PAFServer.PAFServerObj):
    """
    服务对象
    """
    def init(self):
        return True

    def compile(self, task, work, client, current):
        """ 
        """
        pass

#========================================================================================================
#启动server
server = PAF.PAFServer.PAFServer(Compile(), 'ServerConfig.py')
server.init(config.LISTEN_IP, config.LISTEN_PORT)
server.setupPipe()
server.start()
