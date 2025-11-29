{% macro normalize_area(col) %}
case
    when {{ col }} is NULL then NULL
    else round(
        case
            when position(lower({{ col }}), 'acres') > 0
                then toFloat32OrZero(replaceRegexpAll({{ col }}, '[^0-9.]', '')) * 43560
            when position(lower({{ col }}), 'sqft') > 0
                then toFloat32OrZero(replaceRegexpAll({{ col }}, '[^0-9.]', ''))
    end, 2)
end
{% endmacro %}