"""Supply-lot quarantine propagation into finished products, packs and shipments.

Traceability follows what was *actually consumed*: a released blend portion is
the record that a bale went into the batch, and the pellet check / packs /
shipments hang off that blend.  Current yard assignment or later re-clustering
never changes the weighed snapshot, so propagation walks

    supplier lot -> consumed bale -> released blend -> pellet check -> pack -> shipment

Each (lot, product) pair keeps at most one `lot_impacts` row, so retrying a
quarantine/clear never duplicates a disposition, and clearing one lot only
withdraws that lot's own row -- another lot that also hit the same product
keeps its own disposition.  Historical certificates, blend portions and their
cluster/mass snapshots are never rewritten.
"""
from decimal import Decimal
from datetime import datetime,timezone
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from .models import Bale,Blend,BlendPortion
from .quality_models import SupplierLot,LotBale,PelletCheck
from .production_models import PelletPack,BlendShipment,ShipmentPack
from .trace_models import ProcessGate,LotImpact,LotImpactShipment
from .blending import obj

def now():return datetime.now(timezone.utc).isoformat()
GATE='lot-product-gate'

# ---------------------------------------------------------------------------
# serialization gate
# ---------------------------------------------------------------------------
def gate(s):
    """Serialize quarantine propagation against every product-facing action.

    One row, always taken first by both sides.  Quarantine and
    certify/pack/load/dispatch therefore resolve strictly one after the other:
    whichever commits first decides whether an action is refused or a dispatch
    becomes a pending recall.  Callers run in a single DB transaction, so a
    failure leaves no half-written disposition.
    """
    # Existing row: FOR UPDATE serializes both transactions.  First use creates
    # it in a savepoint; a lost race simply re-reads the winner's row.
    row=s.scalar(select(ProcessGate).where(ProcessGate.name==GATE).with_for_update().execution_options(populate_existing=True))
    if row:return row
    try:
        with s.begin_nested():
            s.add(ProcessGate(name=GATE));s.flush()
    except IntegrityError:pass
    return s.scalar(select(ProcessGate).where(ProcessGate.name==GATE).with_for_update().execution_options(populate_existing=True))

# ---------------------------------------------------------------------------
# trace helpers
# ---------------------------------------------------------------------------
def lot_bale_ids(s,lot_id,lock=False):
    q=select(LotBale).where(LotBale.lot_id==lot_id).order_by(LotBale.bale_id)
    if lock:q=q.with_for_update().execution_options(populate_existing=True)
    return [x.bale_id for x in s.scalars(q)]

def affected_lots(s,blend_id,lock=False):
    q=(select(SupplierLot).join(LotBale,LotBale.lot_id==SupplierLot.id)
       .join(BlendPortion,BlendPortion.bale_id==LotBale.bale_id)
       .where(BlendPortion.blend_id==blend_id).order_by(SupplierLot.id))
    if lock:q=q.with_for_update().execution_options(populate_existing=True)
    return list(s.scalars(q).unique())

def affected_products(s,lot_id,lock=False):
    q=(select(PelletCheck).join(BlendPortion,BlendPortion.blend_id==PelletCheck.blend_id)
       .join(LotBale,LotBale.bale_id==BlendPortion.bale_id)
       .where(LotBale.lot_id==lot_id).order_by(PelletCheck.id))
    if lock:q=q.with_for_update().execution_options(populate_existing=True)
    return list(s.scalars(q).unique())

def product_sources(s,check):
    """Consumed bales of a product with their immutable supply-lot attribution."""
    blend=s.get(Blend,check.blend_id);rows=[]
    for p in s.scalars(select(BlendPortion).where(BlendPortion.blend_id==check.blend_id).order_by(BlendPortion.bale_id)):
        bale=s.get(Bale,p.bale_id);link=s.scalar(select(LotBale).where(LotBale.bale_id==p.bale_id))
        lot=s.get(SupplierLot,link.lot_id) if link else None
        rows.append({'bale_id':p.bale_id,'bale_code':bale.code,'kg':float(p.kg),'pvc_snapshot':float(p.pvc_snapshot),
                     'cluster_snapshot':p.cluster_snapshot,'version_snapshot':p.version_snapshot,
                     'lot_id':lot.id if lot else None,'lot_code':lot.code if lot else None,
                     'supplier':lot.supplier if lot else None,'lot_state':lot.state if lot else None})
    return {'check_id':check.id,'blend_id':blend.id,'blend_code':blend.code,'certificate_code':check.certificate_code,
            'certified_by':check.certified_by,'product_state':check.state,'actual_kg':float(check.actual_kg),
            'max_pvc':float(blend.max_pvc),'bales':rows}

def packs_of(s,check_id):
    return list(s.scalars(select(PelletPack).where(PelletPack.check_id==check_id).order_by(PelletPack.id)))

def shipment_links_for_checks(s,check_ids):
    """Shipped/loaded packs grouped by shipment for the given products."""
    if not check_ids:return []
    pack_ids=list(s.scalars(select(PelletPack.id).where(PelletPack.check_id.in_(check_ids))))
    if not pack_ids:return []
    out={}
    for link in s.scalars(select(ShipmentPack).where(ShipmentPack.pack_id.in_(pack_ids)).order_by(ShipmentPack.id)):
        out.setdefault(link.shipment_id,[]).append(link.pack_id)
    return [{'shipment':s.get(BlendShipment,sid),'pack_ids':pids} for sid,pids in sorted(out.items())]

def shipped(shipment):return shipment.state in ('dispatched','received')

def check_ids(s,blend_ids):
    if not blend_ids:return []
    return list(s.scalars(select(PelletCheck.id).where(PelletCheck.blend_id.in_([int(x) for x in blend_ids])).order_by(PelletCheck.id)))

def active_impacts(s,blend_id,lock=False):
    ids=check_ids(s,[blend_id])
    if not ids:return []
    q=select(LotImpact).where(LotImpact.check_id.in_(ids),LotImpact.state!='cleared').order_by(LotImpact.id)
    if lock:q=q.with_for_update().execution_options(populate_existing=True)
    return list(s.scalars(q))

def blocker_summary(s,blend_id,lock=False):
    """Codes of lots that currently make this product untouchable.

    Combines live quarantine state of the consumed bales with active impact
    rows, so a cleared lot cannot silently unblock another lot's disposition.
    Locking reads are used by write paths so the decision sees rows committed
    by a quarantine that held the gate just before this transaction.
    """
    codes={x.code for x in affected_lots(s,blend_id,lock) if x.state=='quarantine'}
    for imp in active_impacts(s,blend_id,lock):
        codes.add(s.get(SupplierLot,imp.lot_id).code)
    return sorted(codes)

def blocked(s,blend_id):return bool(blocker_summary(s,blend_id))

def ensure_product_open(s,blend_id,action):
    codes=blocker_summary(s,blend_id,lock=True)
    if codes:raise HTTPException(409,f'供应批次 {"、".join(codes)} 已隔离，受影响成品不能{action}')
    return True

def ensure_check_open(s,check_id,action):
    return ensure_product_open(s,s.get(PelletCheck,check_id).blend_id,action)

# ---------------------------------------------------------------------------
# disposition writes
# ---------------------------------------------------------------------------
def _detail(s,lot,check,reason,actor,event):
    snap=product_sources(s,check)
    own=[b for b in snap['bales'] if b['lot_id']==lot.id]
    packs=[{'id':x.id,'code':x.code,'kg':float(x.kg),'state':x.state} for x in packs_of(s,check.id)]
    ships=[]
    for item in shipment_links_for_checks(s,[check.id]):
        sh=item['shipment']
        ships.append({'shipment_id':sh.id,'code':sh.code,'customer':sh.customer,'state':sh.state,
                      'manifest_kg':(sh.manifest or {}).get('total_kg')})
    return {'lot_code':lot.code,'supplier':lot.supplier,'reason':reason,'event':event,'actor':actor,'at':now(),
            'bales':own,'certificate_code':check.certificate_code,'packs':packs,'shipments':ships}

def _sync_recalls(s,impact,check):
    """Flag every already-dispatched shipment; the frozen manifest is untouched."""
    for item in shipment_links_for_checks(s,[check.id]):
        sh=item['shipment']
        if not shipped(sh):continue
        link=s.scalar(select(LotImpactShipment).where(LotImpactShipment.impact_id==impact.id,LotImpactShipment.shipment_id==sh.id).with_for_update())
        if link:
            if link.state!='recall_pending':link.state='recall_pending';link.cleared_at=None
            continue
        s.add(LotImpactShipment(impact_id=impact.id,shipment_id=sh.id,state='recall_pending',
                                manifest_snapshot=sh.manifest,created_at=now()))
    s.flush()

def _has_shipped(s,check):return any(shipped(x['shipment']) for x in shipment_links_for_checks(s,[check.id]))

def _apply_one(s,lot,check,reason,actor):
    """Create or reactivate the single disposition for (lot, product)."""
    target='recall_pending' if _has_shipped(s,check) else 'blocked'
    impact=s.scalar(select(LotImpact).where(LotImpact.lot_id==lot.id,LotImpact.check_id==check.id).with_for_update())
    if impact and impact.state in ('blocked','recall_pending'):
        # Retry: never duplicate; still absorb a shipment dispatched since.
        _sync_recalls(s,impact,check)
        return False
    event='reactivated' if impact else 'blocked'
    detail=_detail(s,lot,check,reason,actor,event)
    entry={'event':event,'state':target,'reason':reason,'actor':actor,'at':now()}
    if impact:
        impact.state=target;impact.reason=reason;impact.detail=detail;impact.updated_at=now()
        impact.history=[*(impact.history or []),entry]
    else:
        impact=LotImpact(lot_id=lot.id,check_id=check.id,state=target,reason=reason,detail=detail,
                         history=[entry],created_at=now(),updated_at=now());s.add(impact)
    s.flush();_sync_recalls(s,impact,check)
    return True

def apply_quarantine(s,lot,reason,actor):
    """Create/reactivate one disposition per affected product, idempotently."""
    made=0
    for check in affected_products(s,lot.id,lock=True):
        if _apply_one(s,lot,check,reason,actor):made+=1
    s.flush()
    return made

def apply_clear(s,lot,reason,actor):
    """Withdraw only this lot's dispositions; other lots keep theirs."""
    changed=0
    for impact in list(s.scalars(select(LotImpact).where(LotImpact.lot_id==lot.id,LotImpact.state!='cleared').order_by(LotImpact.id).with_for_update().execution_options(populate_existing=True))):
        impact.state='cleared';impact.updated_at=now()
        impact.history=[*(impact.history or []),{'event':'cleared','reason':reason,'actor':actor,'at':now()}]
        for link in list(s.scalars(select(LotImpactShipment).where(LotImpactShipment.impact_id==impact.id).order_by(LotImpactShipment.id).with_for_update())):
            if link.state!='cleared':link.state='cleared';link.cleared_at=now()
        changed+=1
    s.flush()
    return changed

def sync_product_impacts(s,check,actor):
    """A product registered after a quarantine immediately gets its disposition."""
    made=0
    for lot in affected_lots(s,check.blend_id,lock=True):
        if lot.state=='quarantine' and _apply_one(s,lot,check,'成品登记时供应批次已隔离',actor):made+=1
    return made

# ---------------------------------------------------------------------------
# read views
# ---------------------------------------------------------------------------
def _shipment_disposition(s,link):
    sh=s.get(BlendShipment,link.shipment_id)
    return {'id':sh.id,'code':sh.code,'customer':sh.customer,'state':sh.state,'disposition_state':link.state,
            'manifest_snapshot':link.manifest_snapshot,'dispatched_by':sh.dispatched_by,
            'receipt':sh.receipt_snapshot}

def impact_view(s,x):
    lot=s.get(SupplierLot,x.lot_id);check=s.get(PelletCheck,x.check_id);blend=s.get(Blend,check.blend_id)
    links=list(s.scalars(select(LotImpactShipment).where(LotImpactShipment.impact_id==x.id).order_by(LotImpactShipment.id)))
    return {**obj(x),'lot_code':lot.code,'supplier':lot.supplier,'lot_state':lot.state,
            'blend_code':blend.code,'product_state':check.state,'certificate_code':check.certificate_code,
            'sources':[b for b in product_sources(s,check)['bales'] if b['lot_id']==lot.id],
            'packs':[{'id':p.id,'code':p.code,'kg':float(p.kg),'state':p.state} for p in packs_of(s,check.id)],
            'shipments':[_shipment_disposition(s,l) for l in links]}

def product_view(s,check):
    """Live verification view: sources, every lot touching the product, dispositions."""
    base=product_sources(s,check);lots=[]
    for lot in affected_lots(s,check.blend_id):
        imp=s.scalar(select(LotImpact).where(LotImpact.lot_id==lot.id,LotImpact.check_id==check.id))
        cons=[b for b in base['bales'] if b['lot_id']==lot.id]
        ack_links=list(s.scalars(select(LotImpactShipment).where(LotImpactShipment.impact_id==imp.id).order_by(LotImpactShipment.id))) if imp else []
        lots.append({'lot_id':lot.id,'lot_code':lot.code,'supplier':lot.supplier,'lot_state':lot.state,
                     'impact_id':imp.id if imp else None,'impact_state':imp.state if imp else None,
                     'impact_reason':imp.reason if imp else None,'impact_updated_at':imp.updated_at if imp else None,
                     'consumed_kg':float(sum((Decimal(str(b['kg'])) for b in cons),Decimal(0))),
                     'bales':cons,'history':(imp.history or []) if imp else [],
                     'shipments':[_shipment_disposition(s,l) for l in ack_links]})
    return {**base,'lots':lots,
            'impacts':[impact_view(s,x) for x in s.scalars(select(LotImpact).where(LotImpact.check_id==check.id).order_by(LotImpact.id))],
            'packs':[{'id':p.id,'code':p.code,'kg':float(p.kg),'state':p.state} for p in packs_of(s,check.id)],
            'blocked':blocked(s,check.blend_id),'blocked_by':blocker_summary(s,check.blend_id)}

def all_impacts_view(s):
    products=[product_view(s,x) for x in s.scalars(select(PelletCheck).order_by(PelletCheck.id))]
    relevant=[p for p in products if p['impacts'] or p['blocked']]
    relevant.sort(key=lambda p:(0 if p['blocked'] else 1,-p['check_id']))
    rows=[impact_view(s,x) for x in s.scalars(select(LotImpact).order_by(LotImpact.id.desc()))]
    return {'products':relevant,'impacts':rows}
