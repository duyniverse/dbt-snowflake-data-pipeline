select
    line_item.order_item_key,
    line_item.part_key,
    line_item.line_number,
    line_item.extended_price,
    line_item.discount_percentage,
    orders.order_key,
    orders.customer_key,
    orders.order_date,
    {{ discounted_amount('line_item.extended_price', 'line_item.discount_percentage') }} as item_discount_amount
from {{ ref("stg_tpch_orders") }} as orders -- ref tới model stg
join {{ ref("stg_tpch_lineitem") }} as line_item -- ref tới model stg
  on orders.order_key = line_item.order_key
order by orders.order_date