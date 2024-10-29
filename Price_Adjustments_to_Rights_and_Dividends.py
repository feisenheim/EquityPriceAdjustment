import os.path
import traceback
from datetime import datetime
import numpy as np
import pandas as pd
pd.set_option("mode.chained_assignment", "warn")
import akshare


def price_adjustments_to_rights_and_dividends(tickers, path_prices, path_fhps, path_output):
    # 前复权价格受未来价格走势影响，持续变化保存没有意义。
    # 生成真实涨跌幅（复权乘数）即可。
    if os.path.isfile(path_output + "price_adjustments_factors" + ".csv"):
        f = pd.read_csv(path_output + "price_adjustments_factors" + ".csv", encoding="gbk", index_col=0)
    else:
        f = pd.DataFrame
    for ticker in tickers:
        if os.path.isfile(path_prices + ticker + ".csv"):
            prices = pd.read_csv(path_prices + ticker + ".csv", encoding="gbk")
            prices["前收盘价"] = prices["收盘"].shift(1)
            prices["实际涨跌幅"] = prices["收盘"].pct_change() * 100
            prices.at[0, "实际涨跌幅"] = prices.at[0, "涨跌幅"]
            prices.set_index("日期", inplace=True)
            prices.index = pd.to_datetime(prices.index)

        if os.path.isfile(path_fhps + ticker + ".csv"):
            fhps = pd.read_csv(path_fhps + ticker + ".csv", encoding="gbk", index_col=0)
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

                    price_p = prices.loc[i_index, '前收盘价']
                    ex_dividend = 0 if pd.isna(i_record["现金分红-现金分红比例"]) else i_record["现金分红-现金分红比例"]
                    ex_right = 0 if pd.isna(i_record["送转股份-送转总比例"]) else i_record["送转股份-送转总比例"]
                    prices.loc[i_index, '前收盘价'] = (price_p - ex_dividend/10) / (1 + ex_right/10)
                    prices.loc[i_index, '实际涨跌幅'] = (prices.loc[i_index, '收盘'] / prices.loc[i_index, '前收盘价'] -1)*100
            else:
                pass
        prices["复权因子"] = prices["实际涨跌幅"]/100+1
        prices["日期"] = prices.index
        prices.index = range(len(prices.index))


        prices_update = pd.DataFrame(prices[["日期",]])

        # prices["后复权收盘"] = ""
        # prices["前复权收盘"] = ""
        #
        # hfq = []
        # qfq = []
        # for i in prices.index:
        #     if i == 0:
        #         hfq.insert(0, prices.loc[i, '收盘'])
        #         qfq.insert(0, prices.loc[prices.index[-1], '收盘'])
        #     else:
        #         hfq.insert(len(hfq), hfq[-1] * (1 + prices.loc[i, '调整后涨跌幅']/100))
        #         qfq.insert(0, qfq[0] / (1 + prices.loc[prices.index[-i], '调整后涨跌幅']/100))
        # prices["后复权收盘"] = hfq
        # prices["前复权收盘"] = 前复权收盘
        prices.to_csv(path_output + ticker + ".csv", encoding="gbk")
    return 0

def check_missing_fhps_events(tickers, path_origin, path_hfq, path_qfq, path_fhps):
    #利用AKShare东方财富原始收盘价及后复权收盘价，对比可得时间日期。
    if os.path.isfile(path_fhps + "check_log.csv"):
        check_log = pd.read_csv(path_fhps + "check_log.csv", dtype={"代码":str}, encoding="gbk")
    else:
        check_log = pd.DataFrame(columns=["代码", "最后未记录调整日", "未记录调整日", "所有调整日"])
    check_log.set_index(["代码"], inplace=True)

    for ticker in tickers:
        if os.path.isfile(path_origin + ticker +".csv") and os.path.isfile(path_hfq + ticker +".csv") and os.path.isfile(path_qfq + ticker +".csv"):
            price_origin = pd.read_csv(path_origin + ticker +".csv", encoding="gbk", index_col=0)
            price_hfq = pd.read_csv(path_hfq + ticker + ".csv", encoding="gbk", index_col=0)
            price_qfq = pd.read_csv(path_qfq + ticker + ".csv", encoding="gbk", index_col=0)

            check = pd.DataFrame()

            check["原始收盘"] = price_origin["收盘"]
            check["后复权收盘"] = price_hfq["收盘"]
            check["前复权收盘"] = price_qfq["收盘"]
            check["日期"] = pd.to_datetime(price_origin["日期"])
            check["日期"] = check["日期"].dt.strftime("%Y%m%d")
            check.index = pd.to_datetime(price_origin["日期"])

            check["原始收盘涨跌幅"] = check["原始收盘"].pct_change()
            check["后复权收盘涨跌幅"] = check["后复权收盘"].pct_change()
            check["前复权收盘涨跌幅"] = check["前复权收盘"].pct_change()
            check.loc[check.index[0], "原始收盘涨跌幅"] = 0
            check.loc[check.index[0], "后复权收盘涨跌幅"] = 0
            check.loc[check.index[0], "前复权收盘涨跌幅"] = 0

            # 两项涨跌幅相差大于0.003（0.3%）的，视为相同，即当日发生股价调整。
            check["后复权调整发生日"] = (abs(check["原始收盘涨跌幅"]-check["后复权收盘涨跌幅"]) > 0.003)
            check["前复权调整发生日"] = (abs(check["原始收盘涨跌幅"]-check["前复权收盘涨跌幅"]) > 0.003)
            events_checked = check[check["后复权调整发生日"] == True]
            events_checked = events_checked[events_checked["前复权调整发生日"] == True]

            # 除权除息记录中未出现的日期
            if os.path.isfile(path_fhps + ticker + ".csv"):
                fhps = pd.read_csv(path_fhps + ticker + ".csv", encoding="gbk", index_col=0)
                fhps.set_index("除权除息日", inplace=True)
                events_missing = events_checked[~events_checked.index.isin(fhps.index)]
            else:
                events_missing = events_checked
            if len(events_checked["日期"].tolist()) > 0:
                check_log.loc[ticker, "所有调整日"] = "|".join(events_checked["日期"].tolist())
            else:
                check_log.loc[ticker, "所有调整日"] = ""
            if len(events_missing["日期"].tolist()) > 0:
                check_log.loc[ticker, "未记录调整日"] = "|".join(events_missing["日期"].tolist())
                check_log.loc[ticker, "最后未记录调整日"]= events_missing["日期"].tolist()[-1]
            else:
                check_log.loc[ticker, "未记录调整日"] = ""
                check_log.loc[ticker, "最后未记录调整日"]= ""
        else:
            pass
        print("%s data checked" % ticker)
    check_log.to_csv(path_fhps + "check_log.csv", encoding="gbk")
    return 0

def akshare_fhps_info_update(ticker, path):
    """
    Download Ex-Rights and Ex-Dividends records from AKShare, if records has already been downloaded earlier, cross-check with the latest one from
    AKShare and update local records if there are any new ones.
    :param ticker: ticker of the stock to download.
    :param path: path to read and save records
    :return:
    """
    log = pd.DataFrame(["log messages"])
    try:
        # get fhps records from AKShare
        fhps_new = akshare.stock_fhps_detail_em(symbol=ticker)
    except:
        log_message = [ticker + " : failed to download fhps records from AKShare website."]
        log.loc[len(log)] = log_message

    else:
        if fhps_new.empty:
            log_message = [ticker + " : no fhps records from AKShare website."]
            log.loc[len(log)] = log_message
        else:
            # 如本地已有历史记录，则比对更新缺失项
            if os.path.isfile(path + ticker + ".csv"):
                fhps_local = pd.read_csv(path + ticker + ".csv", encoding="gbk", index_col=0)
                fhps_new.set_index("除权除息日", inplace=True)
                fhps_local.set_index("除权除息日", inplace=True)
                fhps_new.index = pd.to_datetime(fhps_new.index)
                fhps_local.index = pd.to_datetime(fhps_local.index)

                # 合并数据
                fhps = pd.concat([fhps_new, fhps_local])
                fhps = fhps.groupby(fhps.index).last()

                fhps['除权除息日'] = fhps.index
                fhps['除权除息日'] = fhps['除权除息日'].dt.strftime('%Y/%m/%d')
                fhps.index = range(len(fhps.index))
                fhps.sort_values(by=['除权除息日'], ascending=False)
            else:
                # 如本地无记录，则直接保存下载项.
                fhps = fhps_new

            try:
                fhps.to_csv(path + ticker + ".csv", encoding="gbk")
            except:
                log_message = [ticker + " : failed to save records to local."]
                log.loc[len(log)] = log_message
            else:
                log_message = [ticker + " : updated successfully."]
                log.loc[len(log)] = log_message
    print("Ticker %s updated successfully" % ticker)
    return 0



if __name__ == '__main__':
    path = str.replace(os.getcwd(), "\\", "/")
    #akshare_fhps_info_update(["600276"], path + "/fhps/")

    check_missing_fhps_events(["600276"], path + "/stocks/origin/", path + "/stocks/hfq/", path + "/stocks/qfq/", path + "/fhps/")
    #price_adjustments_to_rights_and_dividends(["600276"], path + "/stocks/origin/", path + "/fhps/", path + "/stocks/output/")
