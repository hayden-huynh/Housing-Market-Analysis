{% macro normalize_bed_bath(col) %}
case
    when {{ col }} is NULL then NULL
    else case
        when {{ col }} = 'Studio' then 1
        else toFloat32OrZero(replaceRegexpAll({{ col }}, '[^0-9.]', ''))
    end 
end
{% endmacro %}