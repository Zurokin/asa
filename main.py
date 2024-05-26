from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query, Depends
from sqlalchemy import create_engine, Column, Integer, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
from datetime import datetime
import logging

app = FastAPI()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация базы данных
DATABASE_URL = "sqlite:///./test.db"  # Используйте "postgresql://user:password@localhost/dbname" для PostgreSQL

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Модель данных
class Roll(Base):
    __tablename__ = "rolls"
    id = Column(Integer, primary_key=True, index=True)
    length = Column(Float, nullable=False)
    weight = Column(Float, nullable=False)
    date_added = Column(DateTime, default=datetime.now)
    date_removed = Column(DateTime, nullable=True)

Base.metadata.create_all(bind=engine)

# Pydantic модели
class RollCreate(BaseModel):
    length: float
    weight: float

class RollResponse(BaseModel):
    id: int
    length: float
    weight: float
    date_added: Optional[datetime]
    date_removed: Optional[datetime]

class RollStats(BaseModel):
    added_count: int
    removed_count: int
    avg_length: float
    avg_weight: float
    min_length: float
    max_length: float
    min_weight: float
    max_weight: float
    total_weight: float
    min_gap: float
    max_gap: float

# CRUD операции с рулонами
class CRUDRoll:
    def __init__(self, session_maker):
        self.session_maker = session_maker

    def create_roll(self, roll: RollCreate) -> Roll:
        db_roll = Roll(**roll.dict())
        db_session = self.session_maker()
        try:
            db_session.add(db_roll)
            db_session.commit()
            db_session.refresh(db_roll)
            return db_roll
        finally:
            db_session.close()

    def delete_roll(self, roll_id: int) -> Roll:
        db_session = self.session_maker()
        try:
            db_roll = db_session.query(Roll).filter(Roll.id == roll_id).first()
            if not db_roll:
                raise HTTPException(status_code=404, detail="Roll not found")
            if db_roll.date_removed:
                raise HTTPException(status_code=400, detail="Roll already removed")
            db_roll.date_removed = datetime.now()
            db_session.commit()
            return db_roll
        finally:
            db_session.close()

    def get_roll(self, roll_id: int) -> Roll:
        db_session = self.session_maker()
        try:
            db_roll = db_session.query(Roll).filter(Roll.id == roll_id).first()
            if not db_roll:
                raise HTTPException(status_code=404, detail="Roll not found")
            return db_roll
        finally:
            db_session.close()

    def get_rolls(self, length_min: Optional[float] = None, length_max: Optional[float] = None,
                  weight_min: Optional[float] = None, weight_max: Optional[float] = None,
                  date_added_min: Optional[datetime] = None, date_added_max: Optional[datetime] = None,
                  date_removed_min: Optional[datetime] = None, date_removed_max: Optional[datetime] = None) -> List[Roll]:
        db_session = self.session_maker()
        try:
            query = db_session.query(Roll)
            if length_min is not None:
                query = query.filter(Roll.length >= length_min)
            if length_max is not None:
                query = query.filter(Roll.length <= length_max)
            if weight_min is not None:
                query = query.filter(Roll.weight >= weight_min)
            if weight_max is not None:
                query = query.filter(Roll.weight <= weight_max)
            if date_added_min is not None:
                query = query.filter(Roll.date_added >= date_added_min)
            if date_added_max is not None:
                query = query.filter(Roll.date_added <= date_added_max)
            if date_removed_min is not None:
                query = query.filter(Roll.date_removed >= date_removed_min)
            if date_removed_max is not None:
                query = query.filter(Roll.date_removed <= date_removed_max)
            return query.all()
        finally:
            db_session.close()

    def get_stats(self, start_date: datetime, end_date: datetime) -> RollStats:
        db_session = self.session_maker()
        try:
            rolls_added = db_session.query(Roll).filter(Roll.date_added.between(start_date, end_date)).all()
            rolls_removed = db_session.query(Roll).filter(Roll.date_removed.between(start_date, end_date)).all()

            added_count = len(rolls_added)
            removed_count = len(rolls_removed)

            if added_count > 0:
                avg_length = sum(roll.length for roll in rolls_added) / added_count
                avg_weight = sum(roll.weight for roll in rolls_added) / added_count
                min_length = min(roll.length for roll in rolls_added)
                max_length = max(roll.length for roll in rolls_added)
                min_weight = min(roll.weight for roll in rolls_added)
                max_weight = max(roll.weight for roll in rolls_added)
            else:
                avg_length = avg_weight = min_length = max_length = min_weight = max_weight = 0

            total_weight = sum(roll.weight for roll in rolls_added)

            gaps = [(roll.date_removed - roll.date_added).total_seconds() / 3600 for roll in rolls_removed if
                    roll.date_removed and roll.date_added]

            if gaps:
                min_gap = min(gaps)
                max_gap = max(gaps)
            else:
                min_gap = max_gap = 0

            return RollStats(
                added_count=added_count,
                removed_count=removed_count,
                avg_length=avg_length,
                avg_weight=avg_weight,
                min_length=min_length,
                max_length=max_length,
                min_weight=min_weight,
                max_weight=max_weight,
                total_weight=total_weight,
                min_gap=min_gap,
                max_gap=max_gap
            )
        finally:
            db_session.close()

crud_roll = CRUDRoll(SessionLocal)

def get_db():
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        raise HTTPException(status_code=500, detail="Database connection error")
    finally:
        db.close()

@app.post("/rolls/", response_model=RollResponse)
def create_roll(roll: RollCreate, db: Session = Depends(get_db)):
    try:
        return crud_roll.create_roll(roll)
    except Exception as e:
        logger.error(f"Error creating roll: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.delete("/rolls/{roll_id}", response_model=RollResponse)
def delete_roll(roll_id: int, db: Session = Depends(get_db)):
    try:
        return crud_roll.delete_roll(roll_id)
    except Exception as e:
        logger.error(f"Error deleting roll with id {roll_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/rolls/{roll_id}", response_model=RollResponse)
def get_roll(roll_id: int, db: Session = Depends(get_db)):
    try:
        return crud_roll.get_roll(roll_id)
    except Exception as e:
        logger.error(f"Error fetching roll with id {roll_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/rolls/", response_model=List[RollResponse])
def get_rolls(length_min: Optional[float] = Query(None), length_max: Optional[float] = Query(None),
              weight_min: Optional[float] = Query(None), weight_max: Optional[float] = Query(None),
              date_added_min: Optional[datetime] = Query(None), date_added_max: Optional[datetime] = Query(None),
              date_removed_min: Optional[datetime] = Query(None), date_removed_max: Optional[datetime] = Query(None),
              db: Session = Depends(get_db)):
    try:
        return crud_roll.get_rolls(length_min=length_min, length_max=length_max,
                                   weight_min=weight_min, weight_max=weight_max,
                                   date_added_min=date_added_min, date_added_max=date_added_max,
                                   date_removed_min=date_removed_min, date_removed_max=date_removed_max)
    except Exception as e:
        logger.error(f"Error fetching rolls: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/rolls/stats/", response_model=RollStats)
def get_roll_stats(start_date: datetime, end_date: datetime, db: Session = Depends(get_db)):
    try:
        return crud_roll.get_stats(start_date, end_date)
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")