from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

TransactionType = Literal["INCOME", "EXPENSE"]
TradeAction = Literal["BUY", "SELL"]


class DateRange(BaseModel):
    start: date
    end: date

    @model_validator(mode="after")
    def validate_order(self) -> DateRange:
        if self.start > self.end:
            raise ValueError("The start date must be before the end date")
        return self


class AccountCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    initial_balance: float = 0
    opening_balance_date: date | None = None
    is_visible: bool = True

    @field_validator("name")
    @classmethod
    def clean_name(cls, value: str) -> str:
        return value.strip()


class AccountUpdate(BaseModel):
    initial_balance: float
    opening_balance_date: date | None = None
    is_visible: bool
    revision: int = Field(ge=1)


class TradeInput(BaseModel):
    date: date
    ticker: str = Field(min_length=1, max_length=32)
    name: str = Field(min_length=1, max_length=160)
    action: TradeAction
    quantity: float = Field(gt=0)
    unit_price: float = Field(ge=0)
    fees: float = Field(ge=0, default=0)
    currency: Literal["EUR"] = "EUR"
    account: str = Field(min_length=1, max_length=120)
    comment: str = Field(default="", max_length=500)

    @field_validator("ticker")
    @classmethod
    def clean_ticker(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("name", "account", "comment")
    @classmethod
    def clean_text(cls, value: str) -> str:
        return value.strip()


class TradeUpdate(TradeInput):
    revision: int = Field(ge=1)


class ImportApplyRequest(BaseModel):
    allow_deletions: bool = False


class LifeEvent(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    year: float = Field(gt=0)
    amount: float


class ProjectionRequest(BaseModel):
    current_age: int = Field(ge=18, le=90)
    retirement_age: int = Field(ge=18, le=100)
    start_capital: float = Field(ge=0)
    monthly_savings: float = Field(ge=0)
    monthly_expenses: float = Field(ge=0)
    years: int = Field(ge=1, le=80)
    annual_return_rate: float = Field(ge=-0.5, le=0.5)
    inflation_rate: float = Field(ge=-0.1, le=0.2)
    salary_growth_rate: float = Field(ge=-0.2, le=0.3)
    tax_rate: float = Field(ge=0, le=1)
    volatility: float = Field(ge=0, le=1, default=0.15)
    stop_working_age: int | None = None
    life_events: list[LifeEvent] = Field(default_factory=list)
    show_real: bool = True
    simulations: int = Field(default=300, ge=10, le=5000)
    seed: int | None = 42

    @model_validator(mode="after")
    def validate_ages(self) -> ProjectionRequest:
        if self.retirement_age < self.current_age:
            raise ValueError("Retirement age cannot be lower than current age")
        if self.stop_working_age is not None and not (
            self.current_age <= self.stop_working_age <= self.current_age + self.years
        ):
            raise ValueError("Stop-working age must fall inside the simulation")
        if any(event.year > self.years for event in self.life_events):
            raise ValueError("Life events must fall inside the simulation")
        return self


class ScenarioInput(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    parameters: ProjectionRequest
