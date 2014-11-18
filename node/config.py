#!/usr/bin/env python
# -*- coding: utf-8 -*-  
import os
import socket

#调度服务地址
SCHEDULER_SERVER = [('10.99.16.36', 1111), ('10.44.86.44', 1111)]

#本服务地址
LISTEN_IP = socket.gethostbyname(socket.gethostname())
LISTEN_PORT = 8412

#服务线程数
WORK_THREAD = 30
#回调线程数
CALLBACK_THREAD = 30

# Nginx的服务端口，Web根目录指向OUTPUT
LISTEN_HTTPD_PORT = 8413       

#产出位置
OUTPUT = os.getcwd() + "/output"
OUTPUT_LEN = len(OUTPUT)

#taskid字符串长度,md5算法
TASKID_LEN = 32 

#log中显示给客户端的模块字符串起始位置
#在这里统一定义使代码更简洁
MOD_POS = OUTPUT_LEN + TASKID_LEN + 2

#缓存依赖关系和编译参数位置
DEPS = os.getcwd() + "/deps"

#产出cache地址
#一共有两个产出cache,一个是自己编译的文件,一个是从其它机器下载回来的产出文件
#两个cache的hash方式不一样
#自己编译cache hash = md5("编译器" + "编译参数" + "所有依赖的文件内容md5值")
#下载产出cache hash = md5("文件内容")
CACHE_PATH = os.path.join(os.environ['HOME'], '.ObjectCache')
DOWNLOAD_CACHE = "Download"  #从其它机器下载的产出cache
COMPILE_CACHE = "Compile"   #自己本身编译的cache

HOME = os.environ['HOME']

#源码cach目录
SOURCE_CACHE = os.path.join(os.environ['HOME'], '.SourceCache')
SOURCE_CACHE_ORI = os.path.join(os.environ['HOME'], '.SourceCacheOri')

#任务存储目录
TASK_DIR = "Task"

#编译规则描述文件名
RULER_FILE = "BCLOUD"

#coverage位置
COVERAGE_PATH = "/home/bcloud/BullseyeCoverage/bin"
COVC = COVERAGE_PATH + "/covc"
COVMERGE = COVERAGE_PATH + "/covmerge"
