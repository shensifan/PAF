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
	Date        : 2014-12-19 16:06:11
"""
import re

class ServerInfo(object):
    """
    服务描述
    """
    def __init__(self, namespace, application=None, server=None):
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
