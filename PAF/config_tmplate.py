#!/usr/bin/env python
# -*- coding:utf-8 -*-
#
################################################################################
#
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
    Author  :   dingyingcai(dingyingcai@baidu.com)
    Date    :   2014/11/06 14:28:00
    Desc    :   template config for PAFServer
"""


#########################
# config template for init PAFServer
WORKCOUNT = 2
CALLBACKCOUNT = 20
MAX_Q_SIZE = 10000000

# if send log to logServer
LOG = False
SERVER_ADDR = ('log.bc.baidu.com', 8765)
