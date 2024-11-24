```
docker exec -ti wfc_postgresql psql --user wfc_test -d wfc


wfc=# SELECT
 orders.order_id, orders.customer_id, items.item_id, items.price
FROM
  wfc.orders orders, 
LATERAL (
  SELECT item_id, price
  FROM wfc.order_items
  WHERE order_id = orders.order_id
  ORDER BY price DESC
  LIMIT 2
) AS items;
 order_id | customer_id | item_id | price 
----------+-------------+---------+-------
        1 | user 10     |     103 |    30
        1 | user 10     |     102 |    20
        2 | user 200    |     205 |   200
        2 | user 200    |     204 |   100
        3 | user 3000   |     306 |  3000
(5 rows)



```