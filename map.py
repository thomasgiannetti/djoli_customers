import pandas as pd
import mysql.connector as connection
from operator import attrgetter
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
import folium
from streamlit_folium import st_folium
import streamlit as st


mydb = connection.connect(host="db-djoli-mysql-do-user-14041340-0.b.db.ondigitalocean.com", 
                          database = 'Djoli',
                          user="doadmin", 
                          passwd="AVNS_-9GS1aN10LcIonhOplk",
                          port = 25060,
                          use_pure=True)

query1 = """SELECT 
    o.restaurantID, 
    ROUND(COUNT(DISTINCT(o.orderID))) AS num_orders, 
    ROUND(SUM(o.totalprice)) AS total_ordered, 
    ROUND(AVG(o.totalprice)) AS basket_size
FROM orders o 
JOIN restaurants r ON r.restaurantID = o.restaurantID
GROUP BY o.restaurantID;
"""

query2 = """SELECT 
    restaurantID, 
    ROUND(AVG(num_orders),1) AS avg_weekly_recurrence
FROM (
SELECT o.restaurantID,
    COUNT(DISTINCT(o.orderID)) AS num_orders, 
    CEIL(DATEDIFF(DATE_FORMAT(COALESCE(o.deliverydate, o.date), '%Y-%m-%d'), '2023-01-01') / 7) AS week_number
FROM orders o
GROUP BY CEIL(DATEDIFF(DATE_FORMAT(COALESCE(o.deliverydate, o.date), '%Y-%m-%d'), '2023-01-01') / 7), o.restaurantID) AS output
GROUP BY restaurantID;
"""

query3 = """SELECT 
    restaurantID,
    ((max_ordermonth-min_ordermonth)+1) AS months_activity,
    ROUND((active_months/((max_ordermonth-min_ordermonth)+1)), 1) AS activity_rate
FROM (
SELECT 
    o.restaurantID,
    MAX(CEIL(DATEDIFF(DATE_FORMAT(COALESCE(o.deliverydate, o.date), '%Y-%m-%d'), '2023-01-01')/ 30)) AS max_ordermonth, 
    MIN(CEIL(DATEDIFF(DATE_FORMAT(COALESCE(o.deliverydate, o.date), '%Y-%m-%d'), '2023-01-01')/ 30)) AS min_ordermonth,
    COUNT(DISTINCT(CEIL(DATEDIFF(DATE_FORMAT(COALESCE(o.deliverydate, o.date), '%Y-%m-%d'), '2023-01-01')/ 30))) AS active_months
FROM orders o
GROUP BY o.restaurantID) AS output;"""

query4 = """SELECT 
    o.restaurantID, 
    ROUND(SUM(oi.quantity*p.weight)) AS volume, 
    ROUND((SUM(oi.quantity*p.weight)/COUNT(DISTINCT o.orderID))) AS avg_order_weight
FROM orders o 
JOIN restaurants r ON r.restaurantID = o.restaurantID
LEFT JOIN orderitems oi ON oi.orderID = o.orderID
LEFT JOIN products p ON p.productID = oi.productID
GROUP BY o.restaurantID;"""

query5 = """SELECT *
FROM restaurants r;"""

query6 = """WITH RankedSKUs AS (
    SELECT
        r.restaurantID,
        sk.name AS sku,
        ROW_NUMBER() OVER (PARTITION BY r.restaurantID ORDER BY COUNT(p.sku) DESC) AS sku_row_num
    FROM
        orders o
        JOIN orderitems oi ON o.orderID = oi.orderID
        JOIN products p ON p.productID = oi.productID
        JOIN restaurants r ON r.restaurantID = o.restaurantID
        JOIN categories c ON c.categoryID = p.categoryID
        JOIN sku sk ON sk.sku_code = p.sku
        WHERE c.name != 'FMCG'
    GROUP BY r.restaurantID, sk.name
)
SELECT
    r.restaurantID,
    rc.contact,
    r.name AS restaurant_name,
    rs1.sku AS highest_sku,
    rs2.sku AS second_highest_sku,
    rs3.sku AS third_highest_sku
FROM
    restaurants r
    LEFT JOIN restaurantcontacts rc ON rc.restaurantID = r.restaurantID
    LEFT JOIN RankedSKUs rs1 ON rs1.restaurantID = r.restaurantID AND rs1.sku_row_num = 1
    LEFT JOIN RankedSKUs rs2 ON rs2.restaurantID = r.restaurantID AND rs2.sku_row_num = 2
    LEFT JOIN RankedSKUs rs3 ON rs3.restaurantID = r.restaurantID AND rs3.sku_row_num = 3;"""

df1 = pd.read_sql(query1,mydb)
df2 = pd.read_sql(query2,mydb)
df3 = pd.read_sql(query3,mydb)
df4 = pd.read_sql(query4,mydb)
df5 = pd.read_sql(query5,mydb)
df6 = pd.read_sql(query6,mydb)

merged_df = pd.merge(df1, df2, on='restaurantID')
merged_df = pd.merge(merged_df, df3, on='restaurantID')
merged_df = pd.merge(merged_df, df4, on='restaurantID')
merged_df = pd.merge(merged_df, df5, on='restaurantID')
merged_df = pd.merge(merged_df, df6, on='restaurantID')
merged_df = merged_df.dropna(subset=['latitude', 'longitude'])

def create_map():
    m = folium.Map(location=[5.345317, -4.024429], zoom_start=12)

    for index, row in merged_df.iterrows():
        total_gmv_formatted = "{:,.0f}".format(row['total_ordered'])
        basket_size_formatted = "{:,.0f}".format(row['basket_size'])
        volume_formatted = "{:,.0f}".format(row['volume'])
        order_weight_formatted = "{:,.0f}".format(row['avg_order_weight'])
        activity_rate_formatted = "{:,.0f}".format(row['activity_rate'] * 100)

        iframe_content = (
            f"<b>Restaurant:</b> {row['name']} <br><br>"
            f"<b>Contact:</b> {row['contact']} <br><br>"
            f"<b>Type:</b> {row['type']} <br>"
            f"<b>Cuisine:</b> {row['cuisine']} <br><br>"
            f"<b>KPIs:</b> <br>"
            f"Total GMV: {total_gmv_formatted} <br>"
            f"Basket Size: {basket_size_formatted} <br>"
            f"Number of Orders: {row['num_orders']} <br>"
            f"Average Recurrence: {row['avg_weekly_recurrence']} <br>"
            f"# Active Months: {row['months_activity']} <br>"
            f"Activity Rate: {activity_rate_formatted} % <br>"
            f"Volume: {volume_formatted} kg <br>"
            f"Average Weight of Order: {order_weight_formatted} kg <br><br>"
            f"<b>Top Product 1:</b> {row['highest_sku']} <br>"
            f"<b>Top Product 2:</b> {row['second_highest_sku']} <br>"
            f"<b>Top Product 3:</b> {row['third_highest_sku']} <br>"
        )

        popup = folium.Popup(iframe_content, min_width=300, max_width=300)
    
        if row['type'] == "Formel":
            marker_color = 'orange'
        elif row['type'] == "Semi-Formel":
            marker_color = 'green'
        elif row['type'] == "Informel":
            marker_color = 'black'
        else:
            marker_color = 'gray' 
    
        folium.Marker(location=[row['latitude'], row['longitude']], icon=folium.Icon(color=marker_color, icon='map-marker', prefix='fa'), popup=popup).add_to(m) 
  return m


map = create_map()
st_folium(map, width=1500)
