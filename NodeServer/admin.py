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

bcloud_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, bcloud_dir)
import Util
import PAF

client = PAF.PAFClient.PAFClient(os.path.join(os.path.dirname(__file__), 'config.py'))
t = client.createProxy("Node", (sys.argv[1], 8100))

if sys.argv[2] == "deploy":
    config = dict()
    execfile(sys.argv[3], config)
    server_dir = os.path.abspath(os.path.dirname(sys.argv[3]))
    
    tar_name = os.path.join(server_dir, \
                            "%s.%s.zip" % (os.path.basename(server_dir), time.time()))
    os.system("cd %s;rm -f %s.*.zip" % (server_dir, os.path.basename(server_dir)))
    os.system("cd %s;zip -r %s PAF Util Dependency %s" % \
            (os.path.dirname(server_dir), tar_name, os.path.basename(server_dir)))
    with open(tar_name, "rb") as f:
        data = f.read()
    item = "%s.%s.%s" % (config["Server"].NAMESPACE, \
                        config["Server"].APPLICATION, \
                        config["Server"].SERVICE)
    pprint.pprint("deploy " + t.deploy(item, data))

if sys.argv[2] == "status":
    pprint.pprint(t.status(sys.argv[3]))

if sys.argv[2] == "start":
    pprint.pprint("start " + t.start(sys.argv[3]))

if sys.argv[2] == "stop":
    pprint.pprint("stop " + t.stop(sys.argv[3]))

if sys.argv[2] == "list":
    a = t.list()
    for i in a:
        pprint.pprint(a[i])

client.terminate()
