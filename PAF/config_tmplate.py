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
#工作线程数量
WORKER_COUNT = 2
#作为客户端时的回调线程数量
CALLBACK_COUNT = 20
#队列长度
MAX_Q_SIZE = 10000000

#
STAT_SERVER = ('log.bc.baidu.com', 8765)
