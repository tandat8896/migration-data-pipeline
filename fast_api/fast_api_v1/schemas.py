from pydantic import BaseModel
from datetime import datetime
from typing import Optional, List

class CustomerBase(BaseModel):
    email: str  # Dùng str thuần cho lành, không cần email-validator nữa
    full_name: str
    phone: Optional[str] = None

class CustomerCreate(CustomerBase):
    pass

class CustomerResponse(CustomerBase):
    customer_id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

class OrderCreate(BaseModel):
    customer_id: int
    total_amount: float

class OrderResponse(OrderCreate):
    order_id: int
    class Config:
        from_attributes = True
