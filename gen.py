import pandas as pd
import numpy as np

import warnings
warnings.filterwarnings('ignore')

def _get_future_qty_sum(df, idx):
    """
        把下一次配送之前所有的需求量加起來
    :param df:
    :param idx:
    """
    for index, row in df.loc[idx + 1:].iterrows():
        if row['can_request_order'] == 1:
            future_sum = df.loc[idx + 1:index]['demand'].sum()
            # print('idx: {} -> {} = {}'.format(idx+1, index, future_sum))
            return future_sum
    return df['demand'].iloc[-1]


def compute_optimal_seq(ful_df, cur_stock, min_fulfillment_unit=24, verbose=False):
    """
    計算出最優的補貨序列

    :param ful_df: 輸入dataframe, 包括了兩個columns: demand, can_request_order
    :param cur_stock: 當前商品庫存值
    :param min_fulfillment_unit: 最小配送量
    """
    fill_qtys = []
    shoul_fills = []
    new_stock = []
    for index, row in ful_df.iterrows():
        should_fill = 0
        fill_qty = 0
        new_stock.append(cur_stock)
        today_demand = row['demand']

        # 當前還有庫存
        # 如果需求量 > 庫存量，最多只能賣庫存的商品
        if today_demand > cur_stock:
            remain_stock = 0
        else:
            remain_stock = cur_stock - today_demand

        if row['can_request_order'] == 1 and index < ful_df.shape[0]:
            future_demand = _get_future_qty_sum(ful_df, index)
            if future_demand > cur_stock - today_demand:
                if verbose:
                    print('future_demand: {} > cur_stock: {} - today_demand: {}'
                          .format(future_demand, cur_stock, today_demand))
                should_fill = 1

                # 配貨量是最小配送量的倍數
                fill_qty = int(min_fulfillment_unit * np.ceil((future_demand - remain_stock) / min_fulfillment_unit))

        cur_stock = remain_stock + fill_qty
        shoul_fills.append(should_fill)
        fill_qtys.append(fill_qty)

    ful_df.loc[:, 'should_fill'] = shoul_fills
    ful_df.loc[:, 'fill_qty'] = fill_qtys
    ful_df.loc[:, 'cur_stock'] = new_stock
    return ful_df
