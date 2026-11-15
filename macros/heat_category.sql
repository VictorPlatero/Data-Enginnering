{#- Clasifica la temperatura máxima diaria en categorías térmicas -#}
{% macro heat_category(temp_col) -%}
    case
        when {{ temp_col }} >= 32 then 'Muy caluroso'
        when {{ temp_col }} >= 27 then 'Caluroso'
        when {{ temp_col }} >= 20 then 'Templado'
        when {{ temp_col }} >= 12 then 'Fresco'
        else 'Frío'
    end
{%- endmacro %}
