{% macro normalize_hoa(column_name) %}
case
    when {{ column_name }} IS NULL then NULL
    when {{ column_name }} = 'No HOA Fee' then 0
    else round(
        case
            when position(lower({{ column_name }}), '/yearly') > 0 or position(lower({{ column_name }}), '/annually') > 0
                then toFloat32OrZero(replaceRegexpAll({{ column_name }}, '[^0-9.]', '')) / 12
            when position(lower({{ column_name }}), '/semi') > 0
                then toFloat32OrZero(replaceRegexpAll({{ column_name }}, '[^0-9.]', '')) / 6
            when position(lower({{ column_name }}), '/quarter') > 0
                then toFloat32OrZero(replaceRegexpAll({{ column_name }}, '[^0-9.]', '')) / 3
            when position(lower({{ column_name }}), '/bi-month') > 0
                then toFloat32OrZero(replaceRegexpAll({{ column_name }}, '[^0-9.]', '')) / 2
            else toFloat32OrZero(replaceRegexpAll({{ column_name }}, '[^0-9.]', ''))
        end, 2)
end
{% endmacro %}