#!/usr/local/bin/python3
# coding=utf-8
__author__ = 'chiyuen_woo'
# *******************************************************************
#     Filename @  mongo_bundle.py
#       Author @  chiyuen_woo
#  Create date @  2019-03-31 17:32
#        Email @  huzhy5018@gmail.com
#  Description @  邮件发送
#      license @ (C) Copyright 2011-2017, ShuHao Corporation Limited.
# ********************************************************************


import sys

import pymongo
from logbook import Logger, StreamHandler
from numpy import empty
from pandas import DataFrame, Index, Timedelta, NaT, to_datetime
from trading_calendars import register_calendar_alias, get_calendar
from zipline.utils.cli import maybe_show_progress

client = pymongo.MongoClient("60.205.230.96", 27018)

client.list_database_names()

handler = StreamHandler(sys.stdout, format_string=" | {record.message}")
logger = Logger(__name__)
logger.handlers.append(handler)


def mongo_equities(bundle_param=None, mongo_client=None):
    """
    Generate an ingest function for custom data bundle
    This function can be used in ~/.zipline/extension.py
    to register bundle with custom parameters, e.g. with
    a custom trading calendar.

    Parameters
    ----------
    bundle_param: list of dict, eg. [{"db": "db_name",
    "collections: ["1", "2"]}, {"db": "db_name", "collections: ["1", "2"]},
     {"db": "db_name", "collections: ["1", "2"]}]
    mongo_client : mongo client

    Returns
    -------
    ingest : callable
        The bundle ingest function

    Examples
    --------
    This code should be added to ~/.zipline/extension.py
    .. code-block:: python
       from zipline.data.bundles import csvdir_equities, register
       register('custom-csvdir-bundle',
                csvdir_equities(["daily", "minute"],
                '/full/path/to/the/csvdir/directory'))
    """

    return MongoBundle(bundle_param, mongo_client).ingest


class MongoBundle:
    """
    Wrapper class to call csvdir_bundle with provided
    list of time frames and a path to the csvdir directory
    """

    def __init__(self, bundle_param=None, mongo_client=None):
        self.bundle_param = bundle_param
        self.mongo_db = mongo_client

    def ingest(self,
               environ,
               asset_db_writer,
               minute_bar_writer,
               daily_bar_writer,
               adjustment_writer,
               calendar,
               start_session,
               end_session,
               cache,
               show_progress,
               output_dir):
        mongo_bundle(environ,
                     asset_db_writer,
                     minute_bar_writer,
                     daily_bar_writer,
                     adjustment_writer,
                     calendar,
                     start_session,
                     end_session,
                     cache,
                     show_progress,
                     output_dir,
                     self.bundle_param,
                     self.mongo_db)


# @bundles.register("mongo")
def mongo_bundle(environ,
                 asset_db_writer,
                 minute_bar_writer,
                 daily_bar_writer,
                 adjustment_writer,
                 calendar,
                 start_session,
                 end_session,
                 cache,
                 show_progress,
                 output_dir,
                 bundle_param=None,
                 mongo_client=None):
    """
    Build a zipline data bundle from the directory with mongo db.
    """
    if not mongo_client:
        mongo_client = environ.get('MONGOCLIENT')
        if not mongo_client:
            raise ValueError("MONGOCLIENT environment variable is not set")

    if not mongo_client.list_database_names():
        raise ValueError("%s is not a mongo connection" % mongo_client)

    if not bundle_param:
        raise ValueError("need bundle param")

    divs_splits = {'divs': DataFrame(columns=['sid', 'amount',
                                              'ex_date', 'record_date',
                                              'declared_date', 'pay_date']),
                   'splits': DataFrame(columns=['sid', 'ratio',
                                                'effective_date'])}
    for param in bundle_param:
        if param["db"] not in mongo_client.list_database_names():
            raise ValueError("mongo client: %s don't contain database: %s"
                             % (mongo_client, mongo_db))
        mongo_db = mongo_client[param["db"]]
        symbols = set(param["collections"])\
                  & set(mongo_db.list_collection_names())
        if not symbols:
            raise ValueError("no <symbol> found in %s" % mongo_db)

        dtype = [('start_date', 'datetime64[ns]'),
                 ('end_date', 'datetime64[ns]'),
                 ('auto_close_date', 'datetime64[ns]'),
                 ('symbol', 'object')]
        metadata = DataFrame(empty(len(symbols), dtype=dtype))
        #
        # if tframe == 'minute':
        #     writer = minute_bar_writer
        # else:
        #     writer = daily_bar_writer

        daily_bar_writer.write(_pricing_iter(mongo_db, symbols, metadata,
                                             divs_splits, show_progress),
                               show_progress=show_progress)

        # Hardcode the exchange to "CSVDIR" for all assets and (elsewhere)
        # register "CSVDIR" to resolve to the NYSE calendar, because these
        # are all equities and thus can use the NYSE calendar.
        metadata['exchange'] = "MONGO"

        asset_db_writer.write(equities=metadata)

        divs_splits['divs']['sid'] = divs_splits['divs']['sid'].astype(int)
        divs_splits['splits']['sid'] = divs_splits['splits']['sid'].astype(int)
        adjustment_writer.write(splits=divs_splits['splits'],
                                dividends=divs_splits['divs'])


def _pricing_iter(mongo_db, symbols, metadata, divs_splits, show_progress):
    with maybe_show_progress(symbols, show_progress,
                             label='Loading custom pricing data: ') as it:
        for sid, symbol in enumerate(it):
            logger.debug('%s: sid %s' % (symbol, sid))
            collector = mongo_db[symbol]

            dfr = read_mongo(collector).sort_index()
            # print(dfr)

            start_date = dfr.index[0]
            end_date = dfr.index[-1]

            # The auto_close date is the day after the last trade.
            ac_date = end_date + Timedelta(days=1)
            metadata.iloc[sid] = start_date, end_date, ac_date, symbol

            if 'split' in dfr.columns:
                tmp = 1. / dfr[dfr['split'] != 1.0]['split']
                split = DataFrame(data=tmp.index.tolist(),
                                  columns=['effective_date'])
                split['ratio'] = tmp.tolist()
                split['sid'] = sid

                splits = divs_splits['splits']
                index = Index(range(splits.shape[0],
                                    splits.shape[0] + split.shape[0]))
                split.set_index(index, inplace=True)
                divs_splits['splits'] = splits.append(split)

            if 'dividend' in dfr.columns:
                # ex_date   amount  sid record_date declared_date pay_date
                tmp = dfr[dfr['dividend'] != 0.0]['dividend']
                div = DataFrame(data=tmp.index.tolist(), columns=['ex_date'])
                div['record_date'] = NaT
                div['declared_date'] = NaT
                div['pay_date'] = NaT
                div['amount'] = tmp.tolist()
                div['sid'] = sid

                divs = divs_splits['divs']
                ind = Index(range(divs.shape[0], divs.shape[0] + div.shape[0]))
                div.set_index(ind, inplace=True)
                divs_splits['divs'] = divs.append(div)

            yield sid, dfr


import numpy as np


def read_mongo(collector):
    data = []
    for it in collector.find({}, {"_id": 0}):
        it["volume"] = it["volume"]
        data.append(it)
    df = DataFrame(data)
    df["date"] = to_datetime(df["date"], utc=True)
    df.set_index("date", inplace=True)
    # print(df.index)
    sessions = get_calendar('XSHG').sessions_in_range("19900101", "20181231")
    # print(sessions)
    df = df.reindex(sessions).fillna(method="ffill")
    df.dropna(inplace=True)
    df["volume"] = list(map(lambda x: int(x), df["volume"]))
    # print(df.dtypes)
    return df


# sessions = get_calendar('XSHG').sessions_in_range("19900101", "20181231")


register_calendar_alias("MONGO", "XSHG")


def main():
    mongo_db = client["daily"]["600578.SH"]
    # mongo_db.get_collection("")
    print(read_mongo(mongo_db))


#
#
if __name__ == '__main__':
    main()
