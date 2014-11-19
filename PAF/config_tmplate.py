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
################服务端配置###########################
#命名空间,用于区分部门或者分组
NAMESPACE = "EE"
#app名称
APPLICATION = "PAF"
#服务名称
SERVICE = "stat"
#服务分组,服务内分组,用于区分IDC或者做set
GROUP = None
#工作线程数量
WORKER_COUNT = 20
#队列长度
MAX_Q_SIZE = 10000000
#服务端口
LISTEN_IP = "127.0.0.1"
LISTEN_PORT = 10000
#远程日志服务器地址
LOG_SERVER = None
#node服务器地址,永远是127.0.0.1
NODE_SERVER = ('127.0.0.1', 9999)

################客户端配置###########################
#回调线程数量
CALLBACK_COUNT = 20
#位置服务器地址,用于注册和查询服务
LOCATER_SERVER = ('127.0.0.1', 8001)

################公用配置##########################
#统计服务器
STAT_SERVER = "EE.PAF.stat"
