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
	Date        : 2014-12-19 10:53:21
"""
import os
import sys
import traceback
import pprint
import time

import ServerInfo

bcloud_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, bcloud_dir)
import Util
import PAF

item = ServerInfo.ServerInfo("EE.PAF.test")

client = PAF.PAFClient.PAFClient()
t = client.createProxy("test", ('127.0.0.1', 8412))

print sys.argv[1]

if sys.argv[1] == "deploy":
    pprint.pprint("deploy " + t.deploy(item, "http://xxx"))

if sys.argv[1] == "status":
    pprint.pprint(t.status(item))

if sys.argv[1] == "start":
    pprint.pprint("start " + t.start(item))

if sys.argv[1] == "stop":
    pprint.pprint("stop " + t.stop(item))

time.sleep(1)
