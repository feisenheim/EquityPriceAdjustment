import os.path
import traceback
from datetime import datetime
import numpy as np
import pandas as pd
import akshare
from GlobalSetting import GlobalSetting as GS

def price_adjustments_to_rights_and_dividends(prices:pd.DataFrame, fhps:pd.DataFrame):
    prices["前收盘价"] = prices["收盘"].shift(1)
    prices["调整后涨跌幅"] = prices["收盘"].pct_change()) * 100
    prices.at[0, "调整后涨跌幅"] = prices.at[0, "涨跌幅"]

    prices.set_index("日期", inplace=True)
    prices.index = pd.to_datetime(prices.index)

    if not fhps.empty:
        fhps.set_index("除权除息日", inplace=True)
        fhps.index = pd.to_datetime(fhps.index)
        # 根据除权除息日信息调整“前收盘价”， 计算实际涨跌幅。
        for i_index, i_record in fhps.iterrows():

            if i_index < prices.index[0]: #可能存在上市前除权除息记录
                continue
            if i_index > prices.index[-1]: #尚未进行除权除息的记录
                continue
            i_index = prices.loc[i_index:].index[0] # 除权日可能不在交易日，取除权后最近交易日

            price_p = prices.shift(fill_value=0).loc[i_index, '收盘']

            ex_dividend = 0 if pd.isna(i_record["现金分红-现金分红比例"]) else i_record["现金分红-现金分红比例"]
            ex_right = 0 if pd.isna(i_record["送转股份-送转总比例"]) else i_record["送转股份-送转总比例"]
            prices.loc[i_index, '前收盘价'] = (price_p - ex_dividend/10) / (1 + ex_right/10)
            prices.loc[i_index, '调整后涨跌幅'] = (prices.loc[i_index, '收盘'] / prices.loc[i_index, '前收盘价'] -1)*100
    else:
        pass

    prices["日期"] = prices.index
    prices.index = range(len(prices.index))
    prices["后复权收盘"] = ""
    prices["前复权收盘"] = ""

    hfq = []
    qfq = []
    for i in prices.index:
        if i == 0:
            hfq.insert(0, prices.loc[i, '收盘'])
            qfq.insert(0, prices.loc[prices.index[-1], '收盘'])
        else:
            hfq.insert(len(hfq), hfq[-1] * (1 + prices.loc[i, '调整后涨跌幅']/100))
            qfq.insert(0, qfq[0] / (1 + prices.loc[prices.index[-i], '调整后涨跌幅']/100))
    prices["后复权收盘"] = hfq
    prices["前复权收盘"] = qfq

        # for i in prices.index:
        #      if i == 0:
        #          prices.loc[i, "后复权收盘"] = prices.loc[i, '收盘']
        #          prices.loc[prices.index[-1], "前复权收盘"] = prices.loc[prices.index[-1], '收盘']
        #      else:
        #          prices.loc[i, "后复权收盘"] = prices.loc[i-1, '后复权收盘'] * (1 + prices.loc[i, '调整后涨跌幅']/100)
        #          prices.loc[prices.index[-(i+1)], "前复权收盘"] = prices.loc[prices.index[-i], '前复权收盘'] / (1 + prices.loc[prices.index[-(i+1)], '调整后涨跌幅']/100)
        #
    return prices

def check_missing_fhps_events(prices_origin:pd.DataFrame, prices_hfq:pd.DataFrame, fhps:pd.DataFrame):
    #利用AKShare东方财富原始收盘价及后复权收盘价，对比可得时间日期。
    check = pd.DataFrame()
    check[["涨跌幅", "后复权涨跌幅"]] = [prices_origin["涨跌幅"], prices_hfq["涨跌幅"]]


def akshare_update():
    # error message log
    err_log = pd.DataFrame(["error message"])
    update_log = pd.DataFrame(["update message"])

    err_message = ["price adjustment update"]
    err_log.loc[len(err_log)] = err_message


    if not os.path.isfile(GS.AKSHARE_DATA_PATH + "DICT_ALL_A_TICKERS.csv"):
            err_message = ["Can not find ticker dictionary"]
            err_log.loc[len(err_log)] = err_message
            err_log.to_csv(GS.AKSHARE_DATA_PATH + "err_log_" + datetime.now().strftime("%Y%m%d%H%M%S") + ".txt", sep='\t', index=False)
            return 1
    else:
        stocks = pd.read_csv(GS.AKSHARE_DATA_PATH + "DICT_ALL_A_TICKERS.csv", dtype={"TICKER": str}, encoding="gbk")
        stocks.index = stocks["TICKER"]

        for i in stocks.index:
            #i = "002937"
            prices = pd.DataFrame()
            fhps = pd.DataFrame()
            if os.path.isfile(GS.AKSHARE_DATA_PATH + "hist/origin/" + str(i) + ".csv"):
                prices = pd.read_csv(GS.AKSHARE_DATA_PATH + "hist/origin/" + str(i) + ".csv", encoding="gbk", index_col=0)
            else:
                err_message = [str(i) + " : historical file not found."]
                err_log.loc[len(err_log)] = err_message
                continue

            if os.path.isfile(GS.AKSHARE_DATA_PATH + "hist/fhps/" + str(i) + ".csv"):
                fhps = pd.read_csv(GS.AKSHARE_DATA_PATH + "hist/fhps/" + str(i) + ".csv", encoding="gbk", index_col=0)  # 下载数据
            else:
                err_message = [str(i) + " : fhps file not found."]
                err_log.loc[len(err_log)] = err_message
                fhps = pd.DataFrame()

            try:
                prices_adjusted = price_adjustments_to_rights_and_dividends(prices, fhps)
            except:
                err_message = [str(i) + " : failed to adjust."]
                err_log.loc[len(err_log)] = err_message
                continue

            try:
                prices_adjusted.to_csv(GS.AKSHARE_DATA_PATH + "hist/update/" + str(i) + ".csv", encoding="gbk")
            except:
                err_message = [str(i) + " : can not be saved"]
                err_log.loc[len(err_log)] = err_message
                continue

            print(str(i) + " : updated")

    err_log.to_csv(GS.AKSHARE_DATA_PATH + "err_log_" + datetime.now().strftime("%Y%m%d%H%M%S") + ".txt", sep='\t', index=False)
    pass


if __name__ == '__main__':
    akshare_update()