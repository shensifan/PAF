#!/usr/bin/env python
# -*- coding: utf-8 -*-  
import os
import socket

#本服务地址
LISTEN_IP = socket.gethostbyname(socket.gethostname())
LISTEN_PORT = 8412

#服务线程数
WORKCOUNT = 30
#回调线程数
CALLBACKCOUNT = 30

LOG = False
PAF_LOG = False
LOG_SERVER = ('log.bc.baidu.com', 8765)
