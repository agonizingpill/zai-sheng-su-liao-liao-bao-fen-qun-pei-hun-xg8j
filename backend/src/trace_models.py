"""Persistent lot -> product dispositions.

These tables are additive: no existing column is changed, so create_all keeps
working on a historical database.  `lot_impacts` has one row per
(lot, product) pair and keeps its own event history; reopening a cleared row on
re-quarantine updates the same row instead of growing duplicates.
"""
from sqlalchemy import String,ForeignKey,JSON,UniqueConstraint,Text
from sqlalchemy.orm import Mapped,mapped_column
from .database import Base

class ProcessGate(Base):
    __tablename__='process_gates'
    name:Mapped[str]=mapped_column(String(60),primary_key=True)

class LotImpact(Base):
    __tablename__='lot_impacts'
    __table_args__=(UniqueConstraint('lot_id','check_id'),)
    id:Mapped[int]=mapped_column(primary_key=True)
    lot_id:Mapped[int]=mapped_column(ForeignKey('supplier_lots.id'))
    check_id:Mapped[int]=mapped_column(ForeignKey('pellet_checks.id'))
    state:Mapped[str]=mapped_column(String(20),default='blocked')
    reason:Mapped[str]=mapped_column(Text)
    detail:Mapped[dict]=mapped_column(JSON)
    history:Mapped[list]=mapped_column(JSON)
    created_at:Mapped[str]=mapped_column(String(40))
    updated_at:Mapped[str]=mapped_column(String(40))

class LotImpactShipment(Base):
    __tablename__='lot_impact_shipments'
    __table_args__=(UniqueConstraint('impact_id','shipment_id'),)
    id:Mapped[int]=mapped_column(primary_key=True)
    impact_id:Mapped[int]=mapped_column(ForeignKey('lot_impacts.id'))
    shipment_id:Mapped[int]=mapped_column(ForeignKey('blend_shipments.id'))
    state:Mapped[str]=mapped_column(String(20),default='recall_pending')
    manifest_snapshot:Mapped[dict|None]=mapped_column(JSON,nullable=True)
    created_at:Mapped[str]=mapped_column(String(40))
    cleared_at:Mapped[str|None]=mapped_column(String(40),nullable=True)
