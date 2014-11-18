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
import resource
import socket
import threading
import multiprocessing
import traceback

import log

bcloud_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, bcloud_path)
import PAF

def runCommand(cmd):
    """
    ru = resource.getrusage(resource.RUSAGE_SELF)
    #print ru
    if ru[2] < 600000:
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
    else:
        if threading.current_thread().ident not in procClient:
            procClient[threading.current_thread().ident] = PAF.PAFClientSync((socket.gethostbyname(socket.gethostname()) , 8417))
        return procClient[threading.current_thread().ident].runCommand(cmd)       
    """
    try:
        clnt_p = threading.current_thread().getCmd_Pipe()
        clnt_p.send(cmd)
        return clnt_p.recv()
    except BaseException as e:
        print e, "use local call"
        try:
            t = subprocess.Popen(cmd,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT,
                                 shell=True
                                )
            msg, err = t.communicate()
            retcode = t.wait()
        except BaseException as e:
            retcode = -1
            msg = 'runCommand error for %s: %s' % (str(cmd), str(e))
        return (retcode, msg)


def sys_call(cmd):
    """
    if threading.current_thread().ident not in procClient:
        procClient[threading.current_thread().ident] = PAF.PAFClientSync((socket.gethostbyname(socket.gethostname()) , 8417))
    try:
        ret =  procClient[threading.current_thread().ident].sys_call(cmd)       
    except:
        print "error when call proccall, use os.system"
        ret = os.system(cmd) 
    """
    try:
        clnt_p = threading.current_thread().getSys_Pipe()
        clnt_p.send(cmd)
        return clnt_p.recv()
    except BaseException as e:
        print e, "use local call"
        return os.system(cmd)


def sys_call_proc(s_pipe):
    """
    被提前fork出用于执行os.system的进程代码
    """
    while True:
        try:
            cmd = s_pipe.recv()    
            if cmd == "bcloud:exit":
                return
            #print "will run system:", cmd
            ret = os.system(cmd) 
            s_pipe.send(ret)
        except KeyboardInterrupt:
            os._exit(-1)
        except BaseException as e: 
            traceback.print_exc()
            continue


def runCommand_proc(s_pipe):
    """
    被提前fork出用于执行subprocess的进程代码
    """
    while True:
        try:
            cmd = s_pipe.recv()    
            if cmd == "bcloud:exit":
                return
            #print "will run command:", cmd
            try: 
                t = subprocess.Popen(cmd,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT,
                                     shell=True
                                    )
                msg, err = t.communicate()
                retcode = t.wait()
            except BaseException as e:
                retcode = -1
                msg = 'runCommand error for %s: %s' % (str(cmd), str(e))
            s_pipe.send((retcode, msg))
        except KeyboardInterrupt:
            os._exit(-1)
        except BaseException as e: 
            traceback.print_exc()
            continue
