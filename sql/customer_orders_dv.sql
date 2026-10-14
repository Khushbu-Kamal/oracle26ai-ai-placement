-- Run as ai_lab after setup_schema.py
CREATE JSON RELATIONAL DUALITY VIEW customer_orders_dv AS
SELECT JSON {
  '_id'          : c.customer_id,
  'customerName' : c.customer_name,
  'orders' : [
    SELECT JSON {
      'orderId'     : o.order_id,
      'orderDate'   : o.order_date,
      'status'      : o.status,
      'totalAmount' : o.total_amount
    }
    FROM orders o WITH UPDATE
    WHERE o.customer_id = c.customer_id
  ]
}
FROM customers c WITH UPDATE;
