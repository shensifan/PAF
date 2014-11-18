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
import pprint
conf2 = dict()

execfile("./config.py", dict(), conf2)
pprint.pprint(conf2)
