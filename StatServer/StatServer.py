#!/usr/bin/env python
# -*- coding: utf-8 -*-  

################################################################################
#
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
description: 接口调用统计服务
Authors: shenweizheng(shenweizheng@baidu.com)
Date:    2014-12-22 00:25:50
"""
import os
import sys
import traceback
import pprint
import time
import threading
import signal
import types

bcloud_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, bcloud_dir)
import Util
import PAF

class Stat(PAF.PAFServer.PAFServerObj):
    def init(self):
        return True

#========================================================================================================
config_file = os.path.join(os.path.abspath(os.path.dirname(__file__)), "config.py")
if __name__ == "__main__":
    #启动server
    server = PAF.PAFServer.PAFServer(Stat(), config_file)
    server.start()
