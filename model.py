from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import declarative_base
from pydantic import BaseModel, constr, validator
from datetime import datetime

Base = declarative_base()


class ToolTable(Base):
    __tablename__ = "tools"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    pricing_model = Column(String, index=True)
    description = Column(String, index=True)
    additional_info = Column(String)
    final_url = Column(String, unique=True, index=True)
    url = Column(String, unique=True, index=True)
    last_scraped = Column(DateTime, nullable=False)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "pricing_model": self.pricing_model,
            "description": self.description,
            "additional_info": self.additional_info,
            "final_url": self.final_url,
            "url": self.url,
            # Serialize datetime as ISO 8601 string
            "last_scraped": self.last_scraped.isoformat(),
        }


class ToolResponse(BaseModel):
    id: int
    name: str
    pricing_model: str
    description: str
    additional_info: str
    final_url: str


class ToolCreate(BaseModel):
    name: constr(max_length=255)
    pricing_model: constr(max_length=255)
    description: constr()
    additional_info: constr()
    final_url: constr(max_length=255)
    url: constr(max_length=255)

    @validator("final_url")
    def validate_url(cls, value):
        if not value.startswith("https://"):
            raise ValueError(
                "Invalid URL format. URL must start with 'https://'.")
        return value
