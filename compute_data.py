import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO
import sys
from pathlib import Path
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial

sys.path.append(str(Path(__file__).parent.parent.parent.resolve()))
from algorithms.metrics.data_utils import (
    load_costs,
    load_inventory,
    load_all_stores,
    load_all_goods,
    load_setting,
    load_cm_sku,
    load_require_order,
    load_aa_sku,
    load_refund_out,
)  # noqa
from algorithms.metrics.constants import engine, s3  # noqa


def _process_item(
    item_id,
    store_id,
    store_60_cost,
    store_inventory,
    require_inventory,
    all_goods,
    suggest_order,
    require_order,
    latest_delivery,
    store_cost,
    require_date,
    store,
    store_aa_sku,
    store_refund_out,
):
    un_suggest = 0
    un_req = 0
    un_dlv = 0
    item_cost = store_60_cost[store_60_cost["foreign_item_id"] == item_id]
    sale_days = len(item_cost["date"].drop_duplicates().tolist())
    item_28_cost = item_cost[
        item_cost["date"] > (end - timedelta(days=28)).strftime("%Y-%m-%d")
    ]
    sale_days_in_28 = len(item_28_cost["date"].drop_duplicates().tolist())
    inv = store_inventory[store_inventory["foreign_item_id"] == item_id][
        "quantity"
    ].values
    req_inv = require_inventory[
        (require_inventory["foreign_store_id"] == store_id)
        & (require_inventory["foreign_item_id"] == item_id)
    ]["quantity"].values
    item = all_goods[all_goods["foreign_item_id"] == item_id]
    so = suggest_order[suggest_order["foreign_item_id"] == item_id]
    so_qty = so["fill_qty"].values[0] if not so.empty else 0
    if so_qty == 0:
        un_suggest += 1
    req = require_order[require_order["foreign_item_id"] == item_id]
    req_qty = req["order_qty"].values[0] if not req.empty else 0
    if req_qty == 0:
        un_req += 1
    dlv = latest_delivery[latest_delivery["foreign_item_id"] == item_id]
    dlv_qty = dlv["delivery_qty"].values[0] if not dlv.empty else 0
    if dlv_qty == 0:
        un_dlv += 1
    c = store_cost[
        (store_cost["date"] > (end - timedelta(days=7)).strftime("%Y-%m-%d"))
        & (store_cost["foreign_item_id"] == item_id)
    ]
    _latest_c = c[c["date"] == end.strftime("%Y-%m-%d")]["total_quantity"].values
    latest_c = _latest_c[0] if _latest_c else 0
    warehouse = pd.read_sql(
        f"select * from data_warehouse_{source_id} where date = %(date)s and foreign_item_id = %(foreign_item_id)s limit 1",
        engine,
        params={"date": require_date, "foreign_item_id": item_id},
    )
    return (
        un_suggest,
        un_req,
        un_dlv,
        pd.DataFrame(
            {
                "门店ID": [store_id],
                "门店名称": [store["store_name"].values[0]],
                "商品show_code": [item["show_code"].values[0]],
                "商品名称": [item["item_name"].values[0]],
                "当前库存": [inv[0] if len(inv) > 0 else 0],
                "订货时的库存": [req_inv[0] if len(req_inv) > 0 else 0],
                "60天内售卖天数": [sale_days],
                "28天内售卖天数": [sale_days_in_28],
                "上上次建议单": [so_qty],
                "上上次订货单": [req_qty],
                "上次送货单": [dlv_qty],
                "最近七日均销售额": [sum(c["total_sale"]) / 7],
                "最近七日均销量": [sum(c["total_quantity"]) / 7],
                "是否为 AA 品": [
                    "是" if item_id in store_aa_sku["foreign_item_id"].tolist() else "否"
                ],
                "原因": [
                    store_refund_out[store_refund_out["foreign_item_id"] == item_id][
                        "method"
                    ].values[0]
                    if item_id in store_refund_out["foreign_item_id"].values
                    and "method" in store_refund_out.columns
                    else ""
                ],
                "送货时的大仓库存": [
                    warehouse["stock_qty"].values[0] if not warehouse.empty else 0
                ],
                "今日销量": [latest_c],
            }
        ),
    )


def lacking_rate(source_id, start, end, **kwargs):
    dates = pd.date_range(start=start, end=end)
    print(f"日期范围:{start} ~ {end}")
    cmid = source_id.split("Y", 1)[0]
    # 成本
    costs = load_costs(source_id, dates)
    next_30days = pd.date_range(start=start - timedelta(30), end=start, closed="left")
    _costs_in_next_30days = load_costs(source_id, next_30days)
    costs_in_60days = pd.concat([costs, _costs_in_next_30days])

    # 配置
    setting = load_setting(cmid)
    # 库存
    inventory = load_inventory(source_id, pd.date_range(dates[-1], dates[-1]))
    # 商品
    goods = load_cm_sku(cmid, end)
    item_status_code = setting["item_status_code"]
    if not item_status_code:
        item_status_code = setting["foreign_item_status"]
    goods = goods[goods["busgate"].isin(item_status_code)]
    # 门店
    stores = load_all_stores(source_id, end)
    # 全部商品
    all_goods = load_all_goods(source_id, end)
    # aa
    aa_sku = load_aa_sku(cmid, end)
    # 退货、调拨
    refund_out = load_refund_out(cmid, pd.date_range(end, end))

    sheet1: list = []
    sheet2: list = []
    require_inventorires = {}
    for store_id in setting["foreign_store_ids"]:
        print(f"处理门店:{store_id}...")
        store = stores[stores["foreign_store_id"] == store_id]
        store_cost = costs[costs["foreign_store_id"] == store_id]
        store_60_cost = costs_in_60days[costs_in_60days["foreign_store_id"] == store_id]
        store_inventory = inventory[inventory["foreign_store_id"] == store_id]
        store_goods = goods[goods["foreign_store_id"] == store_id]
        store_aa_sku = aa_sku[aa_sku["foreign_store_id"] == store_id]
        store_refund_out = refund_out[refund_out["foreign_store_id"] == store_id]
        item_ids = store_goods["foreign_item_id"].drop_duplicates().tolist()
        item_inventory = (
            store_inventory.groupby(["foreign_item_id"])
            .agg({"quantity": "sum"})
            .reset_index()
        )
        full_sku = set(item_ids)
        not_empty_sku = set(
            item_inventory[
                (item_inventory["quantity"] != 0)
                & (item_inventory["foreign_item_id"].isin(full_sku))
            ]["foreign_item_id"].tolist()
        )
        empty_sku = full_sku - not_empty_sku
        lacking_rate = len(empty_sku) / len(full_sku)
        latest_delivery = pd.read_sql(
            f"select delivery_qty, delivery_date, foreign_item_id from delivery_{source_id} where delivery_date = (select MAX(delivery_date) as delivery_date from delivery_{source_id} where foreign_store_id = '{store_id}' and delivery_type = '统配出') and foreign_store_id = '{store_id}' and delivery_type = '统配出'",
            engine,
        )
        latest_delivery = (
            latest_delivery.groupby("foreign_item_id")
            .agg({"delivery_qty": "sum", "delivery_date": "first"})
            .reset_index()
        )
        delivery_date = pd.Timestamp(latest_delivery["delivery_date"].values[0])
        require_date = delivery_date - timedelta(1)
        require_order = load_require_order(cmid, require_date, store_id)
        try:
            suggest_date = require_date
            suggest_order = pd.read_csv(
                f"s3://suggest-order/dist/{cmid}/{store_id}/{suggest_date.strftime('%Y-%m-%d')}.csv.gz",
                compression="gzip",
                dtype={"foreign_item_id": str, "show_code": str},
            )
        except Exception:
            suggest_date = require_date - timedelta(1)
            suggest_order = pd.read_csv(
                f"s3://suggest-order/dist/{cmid}/{store_id}/{suggest_date.strftime('%Y-%m-%d')}.csv.gz",
                compression="gzip",
                dtype={"foreign_item_id": str, "show_code": str},
            )
        require_inventory = require_inventorires.get(require_date - timedelta(1))
        if require_inventory is None:
            require_inventory = load_inventory(
                source_id,
                pd.date_range(require_date - timedelta(1), require_date - timedelta(1)),
            )
            require_inventorires[require_date - timedelta(1)] = require_inventory
        un_suggest = 0
        un_req = 0
        un_dlv = 0
        with ThreadPoolExecutor() as exe:
            tasks = []
            for item_id in empty_sku:
                task = exe.submit(
                    _process_item,
                    item_id,
                    store_id,
                    store_60_cost,
                    store_inventory,
                    require_inventory,
                    all_goods,
                    suggest_order,
                    require_order,
                    latest_delivery,
                    store_cost,
                    require_date,
                    store,
                    store_aa_sku,
                    store_refund_out,
                )
                tasks.append(task)
            for r in as_completed(tasks):
                result = r.result()
                un_suggest += result[0]
                un_req += result[1]
                un_dlv += result[2]
                sheet2.append(result[3])
        sheet1.append(
            pd.DataFrame(
                {
                    "门店名称": [store["store_name"].values[0]],
                    "门店ID": [store_id],
                    "门店类型": [
                        "直营" if str(store["property_id"].values[0]) == "2" else "加盟"
                    ],
                    "现门店已缺货 SKU 数": [len(empty_sku)],
                    "门店统配商品全量 SKU 数": [len(full_sku)],
                    "门店缺货率": [lacking_rate],
                    "上上次建议单未建议 SKU 数": [un_suggest],
                    "上上次订货单未订 SKU 数": [un_req],
                    "上次送货单未送货 SKU 数": [un_dlv],
                    "上上次建议单未建议 SKU 数比例": [un_suggest / len(empty_sku)],
                    "上上次订货单未订 SKU 数比例": [un_req / len(empty_sku)],
                    "上次送货单未送货 SKU 数比例": [un_dlv / len(empty_sku)],
                    "建议单时间": [suggest_date.strftime("%Y-%m-%d")],
                    "订货单时间": [require_date.strftime("%Y-%m-%d")],
                    "送货单时间": [delivery_date.strftime("%Y-%m-%d")],
                }
            )
        )

    bio = BytesIO()
    writer = pd.ExcelWriter(bio)
    if sheet1:
        pd.concat(sheet1).to_excel(writer, index=False)
    if sheet2:
        pd.concat(sheet2).to_excel(writer, "Sheet2", index=False)
    writer.save()
    bio.seek(0)
    s3.Bucket("replenish").put_object(
        Body=bio,
        Key=f"so_metrics/stock_out_rate_detail/{cmid}/{end.strftime('%Y-%m-%d')}.xlsx",
    )


source_ids = ["43YYYYYYYYYYYYY", "85YYYYYYYYYYYYY"]
now = datetime.now()
start = now - timedelta(days=30)
end = now - timedelta(days=1)


default_args = {
    "owner": "metrics",
    "start_date": datetime(2018, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
    "email": ["guojiaqi@chaomengdata.com", "lvxiang@chaomengdata.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "provide_context": True,
    "queue": "extract",
    "pool": "pool_size_test",
}


with DAG(
    "stock_out_rate_detail",
    catchup=False,
    schedule_interval="0 5 * * *",
    default_args=default_args,
) as dag:
    begin = DummyOperator(task_id="start")
    for source_id in source_ids:
        watch = PythonOperator(
            task_id=f"{source_id}",
            python_callable=partial(lacking_rate, source_id, start, end),
        )
        begin >> watch


if __name__ == "__main__":
    # from concurrent.futures import ThreadPoolExecutor

    # with ThreadPoolExecutor(4) as exe:
    #     for i in range(2, 21):
    #         now = datetime(2019, 1, i) + timedelta(1)
    #         start = now - timedelta(days=30)
    #         end = now - timedelta(days=1)
    #         exe.submit(lacking_rate, "43YYYYYYYYYYYYY", start, end)
    lacking_rate("43YYYYYYYYYYYYY", start, end)