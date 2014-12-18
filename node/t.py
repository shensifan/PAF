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
	Date        : 2014-11-18 18:48:08
"""
import os
import pprint
import time
import sys
conf2 = dict()

execfile("./config.py", dict(), conf2)
##pprint.pprint(conf2)

class ServerInfo(object):
    def __init__(self, namespace, application, server):
        self.namespace = namespace
        self.application = application
        self.server = server

    def __str__(self):
        return ("%s.%s.%s") % (self.namespace, self.application, self.server)

print "argv = %s" % sys.argv

test = dict()
item1 = ServerInfo("a", "b", "c")
test[item1] = "d"
item2 = ServerInfo("1", "2", "3")
test[item2] = "4"

pprint.pprint(test)

for i in test:
    if i == "a.b.c":
        print i
    else:
        print test[i]

print "a.b.c" in test

time.sleep(2)
print "exit..........."
