from uuid import uuid4

import models
import uvicorn
from fastapi import Depends, FastAPI, HTTPException
from postgres_db import engine, get_db
from pydantic import BaseModel
from sqlalchemy.orm import Session

models.Base.metadata.create_all(bind=engine)

app = FastAPI()


class ItemCreate(BaseModel):
    name: str
    description: str | None = None


class ItemRespone(BaseModel):
    id: str
    name: str
    description: str | None = None

    class Config:
        from_attributes = True


@app.post("/items/", response_model=ItemRespone)
def create_item(item: ItemCreate, db: Session = Depends(get_db)):
    item_id = str(uuid4())

    from models import Item as DBItem

    db_item = DBItem(id=item_id, name=item.name, description=item.description)

    db.add(db_item)
    db.commit()
    db.refresh(db_item)

    return db_item


if __name__ == "__main__":
    uvicorn.run("main:app", host="localhost", port=8000, reload=True)
