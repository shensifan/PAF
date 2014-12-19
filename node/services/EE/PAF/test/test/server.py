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
	Date        : 2014-12-19 11:43:56
"""
import time
import os
import sys
import re
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

    def __hash__(self):
        return hash(self.namespace + self.application + self.server)

    def __eq__(self, other):
        return self.namespace == other.namespace and \
                self.application == other.application and \
                self.server == other.server


item = ServerInfo("EE.PAF.test")
client = PAF.PAFClient.PAFClient()
t = client.createProxy("test", ('127.0.0.1', 8412))
t.register(item)

while(1):
    print "1111111"
    time.sleep(1)
