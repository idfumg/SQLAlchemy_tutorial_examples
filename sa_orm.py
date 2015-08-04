from sqlalchemy import (
    create_engine,
    Column, Integer, String, DateTime,
    Sequence, ForeignKey
)

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, backref

import datetime


Base = declarative_base()

class Customer(Base):
    __tablename__ = 'customers'

    id = Column(Integer,
                Sequence('customer_id_seq'), primary_key=True)
    name = Column(String(20), nullable=False)
    surname = Column(String(20), nullable=False)
    email = Column(String(20), nullable=False)

    def __repr__(self):
        return '(%d, %s, %s, %s)' % (
            self.id, self.name,
            self.surname, self.email)

class OrderProduct(Base):
    __tablename__ = 'order_product'

    order_id = Column(Integer, ForeignKey('orders.id'), primary_key=True)
    product_id = Column(Integer, ForeignKey('products.id'), primary_key=True)

    customers = relationship('Customer', secondary='orders', backref=backref('products'))

    def __repr__(self):
        return '(%d, %d)' % (self.order_id, self.product_id)

class Product(Base):
    __tablename__ = 'products'

    id = Column(Integer, Sequence('product_id_seq'), primary_key=True)
    name = Column(String(64), nullable=False)
    price = Column(Integer, nullable=False)

    def __repr__(self):
        return '(%d, %s, %d)' % (self.id, self.name, self.price)

class Order(Base):
    __tablename__ = 'orders'

    id = Column(Integer, Sequence('order_id_seq'), primary_key=True)
    customer_id = Column(Integer, ForeignKey('customers.id'))
    init_time = Column(DateTime, nullable=False)

    customer = relationship('Customer', backref=backref('orders', order_by=id))
    products = relationship('Product', secondary='order_product', backref=backref('orders'))

    def __repr__(self):
        return '(%d, %d, %s)' % (self.id, self.customer_id, self.init_time)


def db_create(Base):
    Base.metadata.create_all(engine)


def db_insert(session):
    customers = [
        Customer(id=1, name='john', surname='white', email='john@email.com'),
        Customer(id=2, name='joe', surname='black', email='joe@email.com'),
        Customer(id=3, name='fillip', surname='orange', email='fillip@email.com'),
        Customer(id=4, name='nata', surname='purple', email='nata@email.com'),
        Customer(id=5, name='will', surname='brown', email='will@email.com'),
        Customer(id=6, name='paul', surname='blue', email='paul@email.com')
    ]

    session.add_all(customers)

    products = [
        Product(id=1, name='apple', price=20),
        Product(id=2, name='orange', price=30),
        Product(id=3, name='mandarin', price=40),
        Product(id=4, name='potato', price=50),
        Product(id=5, name='cucumber', price=60),
        Product(id=6, name='pear', price=70),
        Product(id=7, name='lemon', price=80)
    ]

    session.add_all(products)

    now = datetime.datetime.now()
    orders = [
        Order(id=1, customer_id=1, init_time=now + datetime.timedelta(days=1)),
        Order(id=2, customer_id=1, init_time=now + datetime.timedelta(days=2)),
        Order(id=3, customer_id=2, init_time=now + datetime.timedelta(days=3)),
        Order(id=4, customer_id=5, init_time=now + datetime.timedelta(days=4)),
        Order(id=5, customer_id=2, init_time=now + datetime.timedelta(days=5)),
        Order(id=6, customer_id=4, init_time=now + datetime.timedelta(days=6)),
        Order(id=7, customer_id=3, init_time=now + datetime.timedelta(days=7)),
        Order(id=8, customer_id=3, init_time=now + datetime.timedelta(days=8))
    ]

    session.add_all(orders)

    order_products = [
        OrderProduct(order_id=1, product_id=2),
        OrderProduct(order_id=1, product_id=4),
        OrderProduct(order_id=1, product_id=6),
        OrderProduct(order_id=2, product_id=4),
        OrderProduct(order_id=3, product_id=7),
        OrderProduct(order_id=3, product_id=1),
        OrderProduct(order_id=4, product_id=6),
        OrderProduct(order_id=5, product_id=3),
        OrderProduct(order_id=6, product_id=1),
        OrderProduct(order_id=6, product_id=4),
        OrderProduct(order_id=7, product_id=5),
        OrderProduct(order_id=7, product_id=6),
        OrderProduct(order_id=8, product_id=1)
    ]

    session.add_all(order_products)


def db_select(session):
    def print_records(records):
        if not records:
            print 'No records.'
            return

        for record in records:
            print record
        print

    # 1. get all customers.
    print_records(
        session.query(Customer).all())

    # 2. get all orders.
    print_records(
        session.query(Order).all())

    # 3. get all products.
    print_records(
        session.query(Product).all())

    # 4. get all orders for a current customer.
    print_records(
        session.query(Order).filter(Order.customer_id == 2))

    # or
    print_records(
        session.query(Customer).filter_by(id=2).one().orders)

    # 5. get all products for a current order.
    print_records(
        session.query(Product).filter(Product.id.in_(
            session.query(OrderProduct.product_id).filter_by(order_id=1))))

    # or
    print_records(
        session.query(Order).filter_by(id=1).one().products)

    # 6. get all products for a current customer.
    customer_2 = session.query(Customer).filter_by(id=2).one()
    for order in customer_2.orders:
        for product in order.products:
            print product
    print

    # or
    print_records(
        session.query(Product).filter(Product.id.in_(
            session.query(OrderProduct.product_id).filter(OrderProduct.order_id.in_(
                session.query(Order.id).filter_by(customer_id=2))))))

    # or
    print_records(
        session.query(Product).join(OrderProduct).join(Order).join(Customer).\
            filter(Customer.id == 2).order_by(Product.id).all())

    # 7. get count of our customers.
    from sqlalchemy.sql import func
    print_records(
        session.query(func.count(Customer.id)))

    # 8. get money amount that customer leaves for us:
    print_records(
        session.query(func.sum(Product.price)).\
            join(OrderProduct).join(Order).join(Customer).\
                filter(Customer.id == 2))

    # 9. get all customers with their orders if exists.
    print_records(
        session.query(Customer, Order.id).join(Order).order_by(Customer.id))

    # 10. get all customers and their orders (and null if not exists).
    print_records(
        session.query(Customer, Order.id).outerjoin(Order).order_by(Customer.id))

    # 11. get the last customer order.
    max_order_time = \
        session.query(func.max(Order.init_time)).\
            filter(Order.customer_id == 2)
    print_records(
        session.query(Order, Customer).\
            filter(Order.init_time == max_order_time,
                   Customer.id == 2))


if __name__ == '__main__':
    engine = create_engine('sqlite:///:memory:')
    Session = sessionmaker(bind=engine)
    session = Session()

    db_create(Base)
    db_insert(session)
    db_select(session)

    session.rollback()
    session.close()
