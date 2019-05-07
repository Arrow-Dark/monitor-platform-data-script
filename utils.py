import pandas as pd
import json


def load_df(ans_df):
    # with open('test_config.json') as f:
    #     config = json.load(f)
    #     ans_df = pd.read_csv(config[usecase_name]['file'],
    #                          dtype={
    #         'demand': int,
    #         'can_request_order': int,
    #         'should_fill': int,
    #         'fill_qty': int,
    #         'cur_stock': int
    #     },
    #         index_col='index'
    #     )
    #mfu = df[usecase_name]['mfu']
    ans_df = ans_df.astype(dtype={
            'demand': int,
            'can_request_order': int,
            'should_fill': int,
            'fill_qty': int,
            'cur_stock': int
        })
    cur_stock = ans_df['cur_stock'].values[0]
    input_df = ans_df[['cmid','foreign_store_id','foreign_store_name','foreign_item_id','foreign_item_name','date','demand', 'can_request_order']]
    #input_df.re
    return input_df.copy(), ans_df, cur_stock
