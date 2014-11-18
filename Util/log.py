#!/usr/bin/env python
# -*- coding: utf-8 -*-  
import os
import sys
import time
import threading
import pprint

if __file__[-4:].lower() in ['.pyc', '.pyo']:
  _srcfile = __file__[:-4] + '.py'
else:
  _srcfile = __file__
_srcfile = os.path.normcase(_srcfile)

console_lock = threading.Lock()

def currentframe():
  """Return the frame object for the caller's stack frame."""
  try:
    raise Exception
  except:
    return sys.exc_info()[2].tb_frame.f_back

def colorprint(color, msg, prefix = True):
    if prefix:
        now = "[" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())) \
            + " " + str(threading.current_thread().ident) + "] "
    else:
        now = ""
    global console_lock
    console_lock.acquire()
    try:
        if color == "RED":
            print "\033[31m%s%s\033[0m" % (now, msg)
            sys.stdout.flush()
            return

        if color == "GREEN":
            print "\033[32m%s%s\033[0m" % (now, msg)
            sys.stdout.flush()
            return

        if color == "YELLOW":
            print "\033[33m%s%s\033[0m" % (now, msg)
            sys.stdout.flush()
            return

        if color == "BLUE":
            print "\033[34m%s%s\033[0m" % (now, msg)
            sys.stdout.flush()
            return

        print "%s%s" % (now, msg)
        sys.stdout.flush()
    finally:
        console_lock.release()


def colorpprint(color, obj):
    global console_lock
    console_lock.acquire()
    try:
        if color == "RED":
            print "\033[31m"
            pprint.pprint(obj)
            print "\033[0m"
            sys.stdout.flush()
            return

        if color == "GREEN":
            print "\033[32m"
            pprint.pprint(obj)
            print "\033[0m"
            sys.stdout.flush()
            return

        if color == "YELLOW":
            print "\033[33m"
            pprint.pprint(obj)
            print "\033[0m"
            sys.stdout.flush()
            return

        if color == "BLUE":
            print "\033[34m"
            pprint.pprint(obj)
            print "\033[0m"
            sys.stdout.flush()
            return

        pprint.pprint(obj)
        sys.stdout.flush()
    finally:
        console_lock.release()

class Log():
  def __init__(self, logfile = None, prefix = None):
    self.logfile = None
    self.handle = None
    self.prefix = prefix
    if logfile != None:
      self.logfile = logfile
      self.handle = file(logfile, "a")

  def findCaller(self):
    """
    Find the stack frame of the caller so that we can note the source
    file name, line number and function name.
    """
    f = currentframe()
    #On some versions of IronPython, currentframe() returns None if
    #IronPython isn't run with -X:Frames.
    if f is not None:
      f = f.f_back
    rv = "(unknown file)", 0, "(unknown function)"
    while hasattr(f, "f_code"):
      co = f.f_code
      filename = os.path.normcase(co.co_filename)
      if filename == _srcfile:
        f = f.f_back
        continue
      rv = (os.path.basename(co.co_filename), f.f_lineno, co.co_name)
      break
    return rv

  def Print(self, msg):
    try:
      fn, lno, func = self.findCaller()
    except ValueError:
      fn, lno, func = ("unknown file"), 0, "(unknown function)"

    now = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    if self.prefix != None:
      msg = "[%s|%s|%s|%d] %s" % (self.prefix, now, fn, lno, msg)
    else:
      msg = "[%s|%s|%d] %s" % (now, fn, lno, msg)

    try:
      if self.handle != None:
        self.handle.write(msg+"\n")
      else:
        print(msg)
        sys.stdout.flush()
    except BaseException, e:
      print(msg)
      sys.stdout.flush()
