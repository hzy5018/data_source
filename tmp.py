#!/usr/local/bin/python3
# coding=utf-8
__author__ = 'chiyuen_woo'
# *******************************************************************
#     Filename @  tmp.py.py
#       Author @  chiyuen_woo
#  Create date @  2019-03-31 19:12
#        Email @  huzhy5018@gmail.com
#  Description @  邮件发送
#      license @ (C) Copyright 2011-2017, ShuHao Corporation Limited.
# ********************************************************************
import talib


def initialize(context):
    context.secs = [symbol('SPY')]

    context.history_depth = 30

    context.iwarmup = 0

    context.BBANDS_timeperiod = 16

    context.BBANDS_nbdevup = 1.819

    context.BBANDS_nbdevdn = 0.470


def handle_data(context, data):
    context.iwarmup = context.iwarmup + 1

    if context.iwarmup <= (context.history_depth + 1):
        return

    dfHistD = history(30, '1d', 'price')

    S = context.secs[0]

    CurP = data[S].price

    BolU, BolM, BolL = talib.BBANDS(

        dfHistD[S].values,

        timeperiod=context.BBANDS_timeperiod,

        nbdevup=context.BBANDS_nbdevup,

        nbdevdn=context.BBANDS_nbdevdn,

        matype=0)

    record(CurP=CurP, BolU=BolU[-1], BolM=BolM[-1], BolL=BolL[-1])

    if CurP < BolL[-1]:

        order_target_percent(S, +0.97)

    else:

        if CurP > BolU[-1]:
            order_target_percent(S, -0.97)

    return
