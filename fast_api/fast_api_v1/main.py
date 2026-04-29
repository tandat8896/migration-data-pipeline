from typing import List

import models
import schemas
import uvicorn
from fastapi import Depends, FastAPI, HTTPException
from postgres_db import engine, get_db
from sqlalchemy.orm import Session

# Khởi tạo bảng
models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="MLOps Migration Lab v4")


@app.post("/customers/", response_model=schemas.CustomerResponse)
def create_customer(customer: schemas.CustomerCreate, db: Session = Depends(get_db)):
    db_customer = models.Customer(**customer.model_dump())
    db.add(db_customer)
    db.commit()
    db.refresh(db_customer)
    return db_customer


@app.get("/customers/", response_model=List[schemas.CustomerResponse])
def get_all_customers(db: Session = Depends(get_db)):
    return db.query(models.Customer).all()


@app.get("/customers/{customer_id}", response_model=schemas.CustomerResponse)
def get_customer(customer_id: int, db: Session = Depends(get_db)):
    res = (
        db.query(models.Customer)
        .filter(models.Customer.customer_id == customer_id)
        .first()
    )
    if not res:
        raise HTTPException(status_code=404, detail="Not found")
    return res


@app.post("/orders/", response_model=schemas.OrderResponse)
def create_order(order: schemas.OrderCreate, db: Session = Depends(get_db)):
    db_order = models.Order(**order.model_dump())
    db.add(db_order)
    db.commit()
    db.refresh(db_order)
    return db_order


@app.get("/orders/", response_model=List[schemas.OrderResponse])
def get_all_orders(db: Session = Depends(get_db)):
    return db.query(models.Order).all()


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
