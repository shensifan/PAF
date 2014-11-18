#!/usr/bin/env python
# -*- coding: utf-8 -*-  

################################################################################
#
# Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
This module provide some common function.

Authors: shenweizheng(shenweizheng@baidu.com)
         liangbao(liangbao@baidu.com)
Date:    2014/08/05 17:23:06
"""

import os
import sys
import hashlib
import subprocess
import urllib

import log

bcloud_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, bcloud_path)

def getFileMd5(path):
    """
    计算文件md5
    参数:
        path : 文件路径
    返回:
        发生错误时返回 "-1"
        正确时返回文件md5的16进制字符串
    """
    try:
        if not os.path.exists(path):
            log.colorprint("RED", "get md5 fail file not exists %s" % path)
            return "-1"

        md5 = hashlib.md5()
        h = open(path, "rb")
        md5.update(h.read())
        h.close()
        return md5.hexdigest()
    except BaseException, ex:
        log.colorprint("RED", "get md5 faile %s" % path)
        return "-1"

def calcMd5(data):
    """
    计算md5
    参数:
        data : 要计算的数据
    返回:
        发生错误时返回 "-1"
        正确时返回数据md5的16进制字符串
    """
    try:
        md5 = hashlib.md5()
        md5.update(data)
        return md5.hexdigest()
    except BaseException as ex:
        log.colorprint("RED", "calc md5 faile %s" % data)
        return "-1"


def runCommand(cmd):
    try:
        t = subprocess.Popen(cmd,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT,
                             shell=True
                            )
        msg, err = t.communicate()
        retcode = t.wait()
    except BaseException, e:
        retcode = -1
        msg = 'runCommand error for %s: %s' % (str(cmd), str(e))

    return (retcode, msg)

def makeParentDirs(target):
    """
    为target创建父目录
    """
    target_dir = os.path.dirname(target)
    return mkdirs(target_dir)


def mkdirs(target_dir):
    """
    创建目录
    """
    if os.path.exists(target_dir):
        return True

    try:
        os.makedirs(target_dir)
    except BaseException, e:
        pass

    if os.path.exists(target_dir):
        return True

    return False


def getFileWithHttp(ip, port, remote_path, local_path):
    """
    通过http协议下载文件
    """
    if not makeParentDirs(local_path):
        log.colorprint("RED", 'create path error %s' % (local_path))
        return False

    url = "http://%s:%s/%s"%(ip, str(port), remote_path)
    f = None
    try:
        remote_fp = urllib.urlopen(url)
        #只有获得资源成功才写文件
        if remote_fp.getcode() == 200:
            try:
                f = file(local_path, 'w')
            except Exception as e:
                log.colorprint("RED", 'open file %s exception : %s' % (local_path, str(e)))
                return False
            data = remote_fp.read(1024 * 1024 * 16)
            while data:
                f.write(data)
                data=remote_fp.read(1024 * 1024 * 16)
        else:
            log.colorprint("RED", 'Get File use HTTP error code: %d' % (remote_fp.getcode()))
    except Exception, e:
        log.colorprint("RED", 'Get File use HTTP error: exception: %s' % (str(e)))
        return False
    finally:
        if f is not None:
            f.close()

    return True
