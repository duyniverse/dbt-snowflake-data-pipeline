{% macro discounted_amount(price, dis_perc, scale=2) %}
    (-1 * {{price}} * {{dis_perc}})::decimal(16, {{ scale }})
{% endmacro %}

-- macro tên discount_amount
-- nhận 3 tham số đầu vào là cột giá, phần trăm chiết khấu, và số thập phân làm tròn (default=2)
-- công thức: discount_amount = -1*giá*chiết_khấu, ép kiểu decimal, làm tròn n chữ số thập phân