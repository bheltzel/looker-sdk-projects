import looker_sdk
from looker_sdk import models

# get all the possible fields from a dataset
def get_field_values(model_name, explore_name):

    sdk = looker_sdk.init31()

    # API Call to pull in metadata about fields in a particular explore
    explore = sdk.lookml_model_explore(
        lookml_model_name=model_name,
        explore_name=explore_name,
        fields="id, name, description, fields",
    )

    my_fields = []

    # Iterate through the field definitions and pull in the description, sql,
    # and other looker tags you might want to include in  your data dictionary.
    if explore.fields and explore.fields.dimensions:
        for dimension in explore.fields.dimensions:
            dim_def = {
                "field_type": "Dimension",
                "view_name": dimension.view_label,
                "field_name": dimension.label_short,
                "type": dimension.type,
                "description": dimension.description,
                "sql": dimension.sql,
            }
            my_fields.append(dim_def)
    if explore.fields and explore.fields.measures:
        for measure in explore.fields.measures:
            mes_def = {
                "field_type": "Measure",
                "view_name": measure.view_label,
                "field_name": measure.label_short,
                "type": measure.type,
                "description": measure.description,
                "sql": measure.sql,
            }
            my_fields.append(mes_def)

    return my_fields

# pass the fields and get SQL
def get_sql(model_name, explore_name, fields):
    sdk = looker_sdk.init31()
    body = models.WriteQuery(
        model=model_name
        ,view=explore_name
        ,fields=fields
        ,pivots=[]
        ,filters={}
        ,sorts=[]
        ,limit=50
    )
    sql = sdk.run_inline_query(result_format='sql', body=body)
    return sql

# get possible fields
fields = get_field_values('thelook', 'order_items')

# TODO: User selects fields

# pass fields to looker, return SQL
sql = get_sql('thelook', 'order_items', ['order_items.sale_price'])

print(sql)

# SELECT
#         order_items.sale_price  AS order_items_sale_price
# FROM ecomm.order_items  AS order_items

# GROUP BY 1
# ORDER BY 1
# LIMIT 50