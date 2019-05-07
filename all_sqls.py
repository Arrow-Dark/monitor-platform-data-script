compute_psd_sql = '''   
        select 
        t1.cmid,t1.foreign_store_id,t1.foreign_item_id,t1.sum_total_sale,t1.psd,t1.lastin_price,t1.psd*t1.lastin_price psd_cost,t1.date,t1.show_code item_show_code
        from
        (select t1.cmid,t1.foreign_store_id,t1.foreign_item_id,max(t1.lastin_price) lastin_price,max(t1.sale_price) sale_price,t1.show_code,
        sum(isexist) days,sum(total_sale) sum_total_sale,sum(total_cost) total_cost,case when days = 0 then 0 else sum_total_sale/days end psd,'{date}'::date as date
        from
        (select t1.cmid,t1.foreign_store_id,t1.foreign_item_id,t1.date,t1.isexist,t1.total_cost,t1.total_sale,t0.lastin_price,t0.sale_price,t0.item_name,t0.item_status,t0.show_code
        from
        (select isnull(t1.cmid,t2.cmid) cmid,isnull(t1.foreign_store_id,t2.foreign_store_id) foreign_store_id,isnull(t1.foreign_item_id,t2.foreign_item_id) foreign_item_id,isnull(t1.date,t2.date) date,
                    case when t1.isexist = 1 or t2.exist = 1 then 1 else 0 end isexist,isnull(total_sale,0) total_sale,isnull(total_cost,0) total_cost
        from
        (select cmid,foreign_store_id,foreign_item_id,date,isnull(quantity,0) quantity,case when isnull(quantity,0) > 0 then 1 else 0 end isexist from inventory_{source_id}  where foreign_store_id in {store_ids} and date > '{date}'::date - 28 and date <= '{date}'::date) t1
        full join
        (select cmid,foreign_store_id,foreign_item_id,date,total_sale,total_cost,case when isnull(total_sale,0) > 0 then 1 else 0 end exist from cost_{source_id}  where foreign_store_id in {store_ids} and date > '{date}'::date - 28 and date <= '{date}'::date) t2
        on t1.cmid = t2.cmid and t1.foreign_store_id = t2.foreign_store_id and t1.foreign_item_id = t2.foreign_item_id and t1.date = t2.date) t1
        left join
        (select cmid,foreign_item_id,barcode,show_code,item_name,item_status,lastin_price,sale_price from chain_goods where cmid = {cmid}) t0
        on t0.cmid = t1.cmid and t0.foreign_item_id = t1.foreign_item_id) t1
        GROUP BY t1.cmid,t1.foreign_store_id,t1.foreign_item_id,t1.show_code
        having t1.show_code in {show_codes}) t1
    '''

read_store_product_sql = '''
    select foreign_store_id,store_show_code,sum(cm_suggest) cm_suggest,sum(store_book) store_book,sum(inv_delivery) inv_delivery,sum(other_reason) other_reason,week_first_day date,EXTRACT('year' from week_first_day) y,EXTRACT('week' from week_first_day) wk
    from
    (select 
    foreign_store_id,store_show_code,case when lost_reason = 1 then 1 else 0 end cm_suggest, case when lost_reason = 2 then 1 else 0 end store_book, case when lost_reason = 3 then 1 else 0 end inv_delivery,case when lost_reason = 4 then 1 else 0 end other_reason,date,date_trunc('week', date::date)::date week_first_day
    from store_product_info_{cmid}) t
    GROUP BY foreign_store_id,store_show_code,week_first_day
    ORDER BY week_first_day
    '''



bd_product_info_sql = '''
    select 
    t1.cmid,t1.foreign_store_id,t1.store_name foreign_store_name,t1.foreign_item_id,t1.barcode,t1.item_name,t1.item_status,t1.show_code item_show_code,delivery_method,t1.store_status store_status,
    t1.days,t1.psd,t1.sum_total_sale total_sale,total_cost,t1.sum_total_sale/28 avg_sale,t1.lastin_price,t1.sale_price,t1.psd*t1.lastin_price psd_cost,t1.date update_date
    from
    (select t1.cmid,t1.foreign_store_id,t1.foreign_item_id,max(t1.lastin_price) lastin_price,max(t1.sale_price) sale_price,t1.item_name,t1.item_status,t1.show_code,t1.store_name,t1.store_status,t1.barcode,t4.alc_way delivery_method,
    sum(isexist) days,sum(total_sale) sum_total_sale,sum(total_cost) total_cost,case when days = 0 then 0 else sum_total_sale/days end psd,CURRENT_DATE as date
    from
    (select t1.cmid,t1.foreign_store_id,t1.foreign_item_id,t1.date,t1.isexist,t1.total_cost,t1.total_sale,t0.lastin_price,t0.sale_price,t0.item_name,t0.item_status,t0.show_code,t3.store_name,t3.store_status,t0.barcode
    from
    (select isnull(t1.cmid,t2.cmid) cmid,isnull(t1.foreign_store_id,t2.foreign_store_id) foreign_store_id,isnull(t1.foreign_item_id,t2.foreign_item_id) foreign_item_id,isnull(t1.date,t2.date) date,
                case when t1.isexist = 1 or t2.exist = 1 then 1 else 0 end isexist,isnull(total_sale,0) total_sale,isnull(total_cost,0) total_cost
    from
    (select cmid,foreign_store_id,foreign_item_id,date,isnull(quantity,0) quantity,case when isnull(quantity,0) > 0 then 1 else 0 end isexist from inventory_{source_id} where date > CURRENT_DATE - 28 and date <= CURRENT_DATE) t1
    full join
    (select cmid,foreign_store_id,foreign_item_id,date,total_sale,total_cost,case when isnull(total_sale,0) > 0 then 1 else 0 end exist from cost_{source_id} where date > CURRENT_DATE - 28 and date <= CURRENT_DATE) t2
    on t1.cmid = t2.cmid and t1.foreign_store_id = t2.foreign_store_id and t1.foreign_item_id = t2.foreign_item_id and t1.date = t2.date) t1
    left join
    (select cmid,foreign_store_id,store_name,store_status from chain_store where cmid = {cmid}) t3
    on t3.cmid=t1.cmid and t3.foreign_store_id = t1.foreign_store_id
    left join
    (select cmid,foreign_item_id,barcode,show_code,item_name,item_status,lastin_price,sale_price from chain_goods where cmid = {cmid}) t0
    on t0.cmid = t1.cmid and t0.foreign_item_id = t1.foreign_item_id) t1
    left join
    (select foreign_store_id,foreign_item_id,alc_way from {gdstore_alc_name} where foreign_store_id != '1000000') t4
    on t4.foreign_store_id = t1.foreign_store_id and t4.foreign_item_id = t1.foreign_item_id
    GROUP BY t1.cmid,t1.foreign_store_id,t1.foreign_item_id,t1.item_name,t1.item_status,t1.show_code,t1.store_name,t1.store_status,t1.barcode,t4.alc_way having t1.foreign_store_id != '1000000') t1
'''


#select cmid,foreign_store_id,foreign_item_id,date,case when mod(date_part('day', date)::INT,2) = 1 then 1 else 2 end as single_or_double_sql,total_quantity sale_quantity from cost_{source_id} where date >= '{sdate}' and date <= '{edate}'
#    select cmid,foreign_store_id,foreign_item_id,date,total_quantity sale_quantity from cost_{source_id} where date >= '{sdate}' and date <= '{edate}'
#    select cmid,foreign_store_id,foreign_item_id,date,total_quantity sale_quantity from cost_{source_id} where date > CURRENT_DATE - 28 and date <= CURRENT_DATE
# actual_sale_sql = '''
#     select distinct isnull(t1.cmid,t2.cmid) cmid,isnull(t1.foreign_store_id,t2.foreign_store_id) foreign_store_id,isnull(t1.foreign_item_id,t2.foreign_item_id) foreign_item_id,isnull(t1.date,t2.date) date,ISNULL(quantity,0) quantity,ISNULL(sale_quantity,0) sale_quantity from
#     (select cmid,foreign_store_id,foreign_item_id,quantity,date from inventory_{source_id} where date > CURRENT_DATE - 28 and date <= CURRENT_DATE) t1
#     FULL JOIN
#     (select cmid,foreign_store_id,foreign_item_id,date,total_quantity sale_quantity from cost_{source_id} where date > CURRENT_DATE - 28 and date <= CURRENT_DATE) t2
#     on t1.cmid=t2.cmid and t1.foreign_store_id = t2.foreign_store_id and t1.foreign_item_id = t2.foreign_item_id and t1.date = t2.date
# '''
# actual_sale_sql = '''
#     select cmid,foreign_store_id,foreign_item_id,quantity actual_quantity,sale_quantity,date,rank_num
#     from
#     (select cmid,foreign_store_id,foreign_item_id,date,quantity,sale_quantity,row_number() over(PARTITION by foreign_store_id,foreign_item_id ORDER BY date) rank_num
#     from 
#     (select distinct isnull(t1.cmid,t2.cmid) cmid,isnull(t1.foreign_store_id,t2.foreign_store_id) foreign_store_id,isnull(t1.foreign_item_id,t2.foreign_item_id) foreign_item_id,isnull(t1.date,t2.date) date,ISNULL(quantity,0) quantity,ISNULL(quantity,0),ISNULL(sale_quantity,0) sale_quantity 
#     from
#     (select cmid,foreign_store_id,foreign_item_id,quantity,date from inventory_{source_id} where date > CURRENT_DATE - 28 and date <= CURRENT_DATE) t1
#     FULL JOIN
#     (select cmid,foreign_store_id,foreign_item_id,total_quantity sale_quantity,date from cost_{source_id} where date > CURRENT_DATE - 28 and date <= CURRENT_DATE) t2
#     on t1.cmid=t2.cmid and t1.foreign_store_id = t2.foreign_store_id and t1.foreign_item_id = t2.foreign_item_id and t1.date = t2.date) t0)
# '''


actual_sale_sql_store_level = '''
    select cmid,foreign_store_id,foreign_item_id,quantity actual_quantity,sale_quantity,date,rank_num
    from
    (select cmid,foreign_store_id,foreign_item_id,date,quantity,sale_quantity,row_number() over(PARTITION by foreign_store_id,foreign_item_id ORDER BY date) rank_num
    from 
    (select distinct isnull(t1.cmid,t2.cmid) cmid,isnull(t1.foreign_store_id,t2.foreign_store_id) foreign_store_id,isnull(t1.foreign_item_id,t2.foreign_item_id) foreign_item_id,isnull(t1.date,t2.date) date,ISNULL(quantity,0) quantity,ISNULL(quantity,0),ISNULL(sale_quantity,0) sale_quantity 
    from
    (select inv.cmid,foreign_store_id,inv.foreign_item_id,quantity,date from inventory_{source_id} inv where date > CURRENT_DATE - 28 and date <= CURRENT_DATE) t1
    FULL JOIN
    (select cst.cmid,foreign_store_id,cst.foreign_item_id,total_quantity sale_quantity,date from cost_{source_id} cst where date > CURRENT_DATE - 28 and date <= CURRENT_DATE) t2
    on t1.cmid=t2.cmid and t1.foreign_store_id = t2.foreign_store_id and t1.foreign_item_id = t2.foreign_item_id and t1.date = t2.date) t0)
    where cmid = {cmid} and foreign_store_id = '{store_id}'
'''


lastest_quantity = '''
    select cmid,foreign_store_id,foreign_item_id,quantity
    from
    (select cmid,foreign_store_id,foreign_item_id,date,quantity,row_number() over(PARTITION by foreign_store_id,foreign_item_id ORDER BY date desc) rank_num
    from inventory_{source_id} where date > CURRENT_DATE - 28 and date <= CURRENT_DATE)
    where rank_num = 1
'''

suggest_order_sql = '''
    select foreign_store_id,foreign_item_id,fill_qty suggest_qty,date from cm_suggest_order_{cmid} where date > CURRENT_DATE - 28 and date <= CURRENT_DATE
'''


identity_sql = """
  select tf.id, tf.username, tf.first_name, ts.stores, ts.level from 
    (select * from identity_userenterpriseidentity as user_identity left join identity_enterpriseidentity as enterprise_idnetity on user_identity.enterprise_identity_id = enterprise_idnetity.id 
        where user_identity.status=1 and enterprise_idnetity.status=1 and enterprise_idnetity.cmid={} and 
        enterprise_idnetity.is_store = false and enterprise_idnetity.is_staff=false and 
        ((enterprise_idnetity.level = 4 and enterprise_idnetity.identity_lv4_name not like '%店长%') or 
        (enterprise_idnetity.level = 3 and enterprise_idnetity.identity_lv3_name not like '%店长%') or 
        (enterprise_idnetity.level = 2 and enterprise_idnetity.identity_lv2_name not like '%店长%') or (enterprise_idnetity.level = 1) or
        (enterprise_idnetity.level = 5 and enterprise_idnetity.identity_lv5_name not like '%店长%')) order by enterprise_idnetity.level) as ts inner join
    (select * from user_cmuser where cmid={} and is_staff=false) as tf on ts.user_id = tf.id
"""

enterprise_sql = """
    select distinct(cmid) from permission_enterpriseconf where cmid not in (1);;
"""

stores_sql = """
    select showcode from permission_storeresource where cmid={};
"""