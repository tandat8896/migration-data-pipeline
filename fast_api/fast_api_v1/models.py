from postgres_db import Base
from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func


class Customer(Base):
    __tablename__ = "customers"
    customer_id = Column(Integer, primary_key=True, index=True)
    email = Column(String(100), unique=True, nullable=False)
    full_name = Column(String(200), nullable=False)
    phone = Column(String(20))
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now(), server_default=func.now())

    orders = relationship("Order", back_populates="owner", cascade="all, delete-orphan")


class Order(Base):
    __tablename__ = "orders"
    order_id = Column(Integer, primary_key=True, index=True)
    customer_id = Column(
        Integer, ForeignKey("customers.customer_id", ondelete="CASCADE")
    )
    total_amount = Column(Float, default=0.0)

    owner = relationship("Customer", back_populates="orders")
