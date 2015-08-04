import sqlite3
import datetime


def db_create(curr):
    curr.execute('''CREATE TABLE CUSTOMERS (
ID NUMBER(32) NOT NULL,
NAME VARCHAR2(20) NOT NULL,
SURNAME VARCHAR2(20) NOT NULL,
EMAIL VARCHAR2(255) NOT NULL
    );''')

    curr.execute('''CREATE TABLE PRODUCTS (
ID NUMBER(32) NOT NULL,
NAME VARCHAR2(64) NOT NULL,
PRICE NUMBER(12) NOT NULL
    );''')

    curr.execute('''CREATE TABLE ORDER_PRODUCT (
ORDER_ID NUMBER(32) NOT NULL,
PRODUCT_ID NUMBER(32) NOT NULL
    );''')

    curr.execute('''CREATE TABLE ORDERS (
ID NUMBER(32) NOT NULL,
CUSTOMER_ID NUMBER(32) NOT NULL,
INIT_TIME DATETIME NOT NULL
    );''')


def db_insert(curr):
    customers = [
        (1, 'john', 'white', 'john@email.com'),
        (2, 'joe', 'black', 'joe@email.com'),
        (3, 'fillip', 'orange', 'fillip@email.com'),
        (4, 'nata', 'purple', 'nata@email.com'),
        (5, 'will', 'brown', 'will@email.com'),
        (6, 'paul', 'blue', 'paul@email.com')
    ]

    curr.executemany(
        'INSERT INTO customers VALUES (?, ?, ?, ?)',
        customers)

    products = [
        (1, 'apple', 20),
        (2, 'orange', 30),
        (3, 'mandarin', 40),
        (4, 'potato', 50),
        (5, 'cucumber', 60),
        (6, 'pear', 70),
        (7, 'lemon', 80)
    ]

    curr.executemany(
        'INSERT INTO products VALUES (?, ?, ?)',
        products)

    now = datetime.datetime.now()
    orders = [
        (1, 1, now + datetime.timedelta(days=1)),
        (2, 1, now + datetime.timedelta(days=2)),
        (3, 2, now + datetime.timedelta(days=3)),
        (4, 5, now + datetime.timedelta(days=4)),
        (5, 2, now + datetime.timedelta(days=5)),
        (6, 4, now + datetime.timedelta(days=6)),
        (7, 3, now + datetime.timedelta(days=7)),
        (8, 3, now + datetime.timedelta(days=8))
    ]

    curr.executemany(
        'INSERT INTO ORDERS VALUES (?, ?, ?)',
        orders)

    order_product = [
        (1, 2),
        (1, 4),
        (1, 6),
        (2, 4),
        (3, 7),
        (3, 1),
        (4, 6),
        (5, 3),
        (6, 1),
        (6, 4),
        (7, 5),
        (7, 6),
        (8, 1)
    ]

    curr.executemany(
        'INSERT INTO ORDER_PRODUCT VALUES (?, ?)',
        order_product)


def db_select(curr):
    def select_and_print(text):
        records = curr.execute(text)

        if records.rowcount == 0:
            print 'No records.'
            return

        for record in records:
            print record
        print

    # 1. get all customers.
    select_and_print(
        'SELECT * FROM customers')

    # 2. get all orders.
    select_and_print(
        'SELECT * FROM orders')

    # 3. get all products.
    select_and_print(
        'SELECT * FROM products')

    # 4. get all orders for a current customer.
    select_and_print(
        'SELECT * FROM orders WHERE customer_id = 2')

    # 5. get all products for a current order.
    select_and_print(
        '''
        SELECT * FROM products WHERE id IN
          (SELECT product_id FROM order_product
             WHERE order_id = 1)
        ''')

    # 6. get all products for a current customer.
    select_and_print(
        '''
        SELECT * FROM products WHERE id IN
          (SELECT product_id FROM order_product WHERE order_id IN
               (SELECT id FROM orders WHERE customer_id = 2))
        ''')

    # 7. get count of our customers.
    select_and_print('SELECT count(*) from customers')

    # 8. get money amount that customer leaves for us:
    select_and_print(
        '''
        SELECT sum(price) FROM products WHERE id IN
          (SELECT product_id FROM order_product WHERE order_id IN
             (SELECT id FROM orders WHERE customer_id = 2))
        ''')

    # 9. get all customers with their orders if exists.
    select_and_print(
        '''
        SELECT customers.id,
               customers.name,
               customers.surname,
               orders.id
          FROM customers, orders
            WHERE customers.id = orders.customer_id
        ''')

    # 10. get all customers and their orders (and null if not exists).
    select_and_print(
        '''
        SELECT customers.id,
               customers.name,
               customers.surname,
               orders.id
          FROM customers
            LEFT JOIN orders
              ON customers.id = orders.customer_id
        ''')

    # 11. get the last customer order.
    select_and_print(
        '''
        SELECT * FROM orders, customers
          WHERE customers.id = orders.customer_id
            AND customer_id = 2
            AND orders.init_time =
             (SELECT max(init_time) FROM orders
                WHERE customer_id = 2)
        ''')


if __name__ == '__main__':
    conn = sqlite3.connect('my.db')
    conn.isolation_level = None
    curr = conn.cursor()
    curr.execute('begin')

    db_create(curr)
    db_insert(curr)
    db_select(curr)

    conn.rollback()
    conn.close()
