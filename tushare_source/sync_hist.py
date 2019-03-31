#!/usr/local/bin/python3
# coding=utf-8
__author__ = 'chiyuen_woo'
# *******************************************************************
#     Filename @  sync_hist.py
#       Author @  chiyuen_woo
#  Create date @  2019-03-31 16:21
#        Email @  huzhy5018@gmail.com
#  Description @  邮件发送
#      license @ (C) Copyright 2011-2017, ShuHao Corporation Limited.
# ********************************************************************

import tushare as ts
import click
import pymongo

mongo_client = pymongo.MongoClient("60.205.230.96", 27018)
daily = mongo_client["daily"]

API_KEY = "64a7e8ae7f717d38c6c44606a51e120f06f2f25a08c5e2bcb3197cb3"
ts.set_token(API_KEY)

pro = ts.pro_api()


def get_all_stocks(show_progress=True):
    # get all stocks code
    if show_progress:
        click.echo("获取股票基础信息")
    ts_symbols = pro.stock_basic(
        exchange='',
        list_status='L',
        fields='ts_code'
    )
    if show_progress:
        click.echo("写入股票列表")
    symbols = set()
    i = 0
    for index, row in ts_symbols.iterrows():
        i = i + 1
        symbols.add(row["ts_code"])
        # if i > 2:
        #     break
    return symbols


def get_all_stocks_data(stock_list, start, end, show_progress=True):
    if show_progress:
        for stock_code in stock_list:
            click.echo("获取股票: %s" % stock_code)
            df = ts.pro_bar(pro_api=pro,
                            ts_code=stock_code,
                            adj='hfq',
                            start_date=start, end_date=end)
            df = df[["trade_date",
                     "open", "high", "low", "close",
                     "vol"]]
            df.columns = ["date", "open", "high", "low", "close", "volume"]
            df["adjusted"] = df["close"]
            collector = daily[stock_code]
            collector.insert_many(df.to_dict("records"))
            # print(collector.list_indexes())
            # create index for date
            collector.create_index([("date", 1)], unique=True)
            # print(df)


def main():
    stock_list = get_all_stocks()
    get_all_stocks_data(stock_list, "19900101", "20181231")


if __name__ == '__main__':
    main()