from sqlalchemy import (
    create_engine, MetaData, Table,
    Column, Integer, Sequence,
    String, ForeignKey, DateTime,
    select
)

from sqlalchemy.sql import and_

import datetime


def db_create(conn, metadata):
    customers = Table('customers', metadata,
                      Column('id', Integer(),
                             Sequence('customer_id_seq'),
                             primary_key=True),
                      Column('name', String(20), nullable=False),
                      Column('surname', String(20), nullable=False),
                      Column('email', String(255), nullable=False))
    products = Table('products', metadata,
                     Column('id', Integer(),
                            Sequence('products_id_seq'),
                            primary_key=True),
                     Column('name', String(64), nullable=False),
                     Column('price', Integer(), nullable=False))
    orders = Table('orders', metadata,
                   Column('id', Integer(),
                          Sequence('orders_id_seq'),
                          primary_key=True),
                   Column('customer_id', Integer, ForeignKey('customers.id')),
                   Column('init_time', DateTime(), nullable=False))
    order_product = Table('order_product', metadata,
                          Column('order_id', Integer, ForeignKey('orders.id')),
                          Column('product_id', Integer, ForeignKey('products.id')))

    return {
        'customers': customers,
        'products': products,
        'orders': orders,
        'order_product': order_product
    }


def db_insert(conn, tables):
    customers = [
        {'name': 'john', 'surname': 'white', 'email': 'john@email.com'},
        {'name': 'joe', 'surname': 'black', 'email': 'joe@email.com'},
        {'name': 'fillip', 'surname': 'orange', 'email': 'fillip@email.com'},
        {'name': 'nata', 'surname': 'purple', 'email': 'nata@email.com'},
        {'name': 'will', 'surname': 'brown', 'email': 'will@email.com'},
        {'name': 'paul', 'surname': 'blue', 'email': 'paul@email.com'}
    ]

    conn.execute(tables['customers'].insert(), customers)

    products = [
        {'name': 'apple', 'price': 20},
        {'name': 'orange', 'price': 30},
        {'name': 'mandarin', 'price': 40},
        {'name': 'potato', 'price': 50},
        {'name': 'cucumber', 'price': 60},
        {'name': 'pear', 'price': 70},
        {'name': 'lemon', 'price': 80}
    ]

    conn.execute(tables['products'].insert(), products)

    import datetime
    from datetime import timedelta
    now = datetime.datetime.now()
    orders = [
        {'customer_id': 1, 'init_time': now + timedelta(days=1)},
        {'customer_id': 1, 'init_time': now + timedelta(days=2)},
        {'customer_id': 2, 'init_time': now + timedelta(days=3)},
        {'customer_id': 5, 'init_time': now + timedelta(days=4)},
        {'customer_id': 2, 'init_time': now + timedelta(days=5)},
        {'customer_id': 4, 'init_time': now + timedelta(days=6)},
        {'customer_id': 3, 'init_time': now + timedelta(days=7)},
        {'customer_id': 3, 'init_time': now + timedelta(days=8)}
    ]

    conn.execute(tables['orders'].insert(), orders)

    order_product = [
        {'order_id': 1, 'product_id': 2},
        {'order_id': 1, 'product_id': 4},
        {'order_id': 1, 'product_id': 6},
        {'order_id': 2, 'product_id': 4},
        {'order_id': 3, 'product_id': 7},
        {'order_id': 3, 'product_id': 1},
        {'order_id': 4, 'product_id': 6},
        {'order_id': 5, 'product_id': 3},
        {'order_id': 6, 'product_id': 1},
        {'order_id': 6, 'product_id': 4},
        {'order_id': 7, 'product_id': 5},
        {'order_id': 7, 'product_id': 6},
        {'order_id': 8, 'product_id': 1}
    ]

    conn.execute(tables['order_product'].insert(), order_product)


def db_select(conn, tables):
    def print_records(select_expr):
        records = conn.execute(select_expr)

        if records.rowcount == 0:
            print 'No records.'
            return

        for record in records:
            print record
        print

    # for more code clarity.
    customers = tables['customers']
    products = tables['products']
    orders = tables['orders']
    order_product = tables['order_product']


    # 1. get all customers.
    print_records(
        select([customers]))

    # 2. get all orders.
    print_records(
        select([orders]))

    # 3. get all products.
    print_records(
        select([products]))

    # 4. get all orders for a current customer.
    print_records(
        select([orders]).where(orders.c.customer_id == 2))

    # 5. get all products for a current order.
    print_records(
        select([products]).where(products.c.id.in_(
            select([order_product.c.product_id]).\
                where(order_product.c.order_id == 1))))

    # 6. get all products for a current customer.
    print_records(
        select([products]).\
        where(products.c.id.in_(
            select([order_product.c.product_id]).\
            where(order_product.c.order_id.in_(
                select([orders.c.id]).\
                where(orders.c.customer_id == 2))))))
    # or

    order_ids = select([orders.c.id]).where(orders.c.customer_id == 2)
    product_ids = select([order_product.c.product_id]).\
                         where(order_product.c.order_id.in_(order_ids))
    print_records(
        select([products]).where(products.c.id.in_(product_ids)))

    # 7. get count of our customers.
    from sqlalchemy.sql import func
    print_records(
        select([func.count(customers).label('count')]))

    # 8. get money amount that customer leaves for us:
    order_ids = select([orders.c.id]).where(orders.c.customer_id == 2)
    product_ids = select([order_product.c.product_id]).\
                  where(order_product.c.order_id.in_(order_ids))
    print_records(
        select([func.sum(products.c.price)]).where(products.c.id.in_(product_ids)))

    # 9. get all customers with their orders if exists.
    print_records(
        select([customers,
                orders.c.id]).where(customers.c.id == orders.c.customer_id).\
                    order_by(customers.c.id))

    # 10. get all customers and their orders (and null if not exists).
    print_records(
        select([customers,
                orders.c.id]).select_from(customers.outerjoin(orders)))

    # 11. get the last customer order.
    max_order_time = select([func.max(orders.c.init_time)]).\
                         where(orders.c.customer_id == 2)
    print_records(
        select([orders, customers]).\
            where(
                and_(
                    customers.c.id == 2,
                    orders.c.init_time == max_order_time)))

if __name__ == '__main__':
    engine = create_engine('sqlite:///:memory:')
    conn = engine.connect()
    metadata = MetaData()
    transaction = conn.begin()

    tables = db_create(conn, metadata)
    metadata.create_all(engine)
    db_insert(conn, tables)
    db_select(conn, tables)

    transaction.rollback()
    conn.close()
