from sqlalchemy.orm import Session
from sqlalchemy import func, and_, extract
from app.models import Document, Exception, Alert
from app.schemas import DocumentCreate, DocumentUpdate, DashboardInsights, KPIMetric, UtilizationTrend, CategorySplit
from app.services.document_linking_service import DocumentLinkingService
import re
from typing import List, Optional, Dict, Any, Tuple
import uuid
from datetime import datetime, timedelta
import json
import os


class DocumentService:
    def __init__(self, db: Session):
        self.db = db
    
    def create_document(self, document: DocumentCreate) -> Document:
        normalized_category = document.category
        client_lower = (document.client or "").lower()
        if normalized_category in ["Client PO", "Vendor PO"] and client_lower in ["google llc", "platform clients", "emb global"]:
            normalized_category = "Service Agreement"

        db_document = Document(
            id=str(uuid.uuid4()),
            **{**document.dict(), "category": normalized_category}
        )
        db_document.msa_number = self._normalize_msa_value(document.msa_number)
        self.db.add(db_document)
        self.db.commit()
        self.db.refresh(db_document)
        return db_document
    
    def get_document(self, document_id: str) -> Optional[Document]:
        return self.db.query(Document).filter(Document.id == document_id).first()
    
    def get_documents(self, skip: int = 0, limit: int = 100) -> List[Document]:
        return self.db.query(Document).offset(skip).limit(limit).all()

    def _load_documents_with_metadata(self) -> Tuple[List[Document], datetime, bool]:
        documents = self.db.query(Document).all()
        updates_made = False
        now = datetime.utcnow()

        for doc in documents:
            normalized_stored = self._normalize_msa_value(doc.msa_number)
            if normalized_stored and normalized_stored != doc.msa_number:
                doc.msa_number = normalized_stored
                msa_key = normalized_stored
                updates_made = True
            else:
                msa_key = self._resolve_msa_number(doc)
            if msa_key and doc.msa_number != msa_key:
                doc.msa_number = msa_key
                updates_made = True

            if doc.category in ["Client PO", "Vendor PO"] and doc.po_number:
                normalized_title = doc.po_number.strip()
                if normalized_title and doc.title != normalized_title:
                    doc.title = normalized_title
                    updates_made = True
            elif doc.category in ["Client Invoice", "Vendor Invoice"] and doc.invoice_number:
                normalized_title = doc.invoice_number.strip()
                if normalized_title and doc.title != normalized_title:
                    doc.title = normalized_title
                    updates_made = True

        if updates_made:
            self.db.commit()

        return documents, now, updates_made

    def get_msa_buckets(self) -> Dict[str, Any]:
        documents, now, _ = self._load_documents_with_metadata()

        buckets: Dict[str, Dict] = {}

        for doc in documents:
            msa_key = doc.msa_number or self._resolve_msa_number(doc)
            msa_key = self._normalize_msa_value(msa_key)
            if not msa_key:
                continue

            bucket = buckets.setdefault(
                msa_key,
                {
                    "msa_number": msa_key,
                    "msa_documents": [],
                    "po_documents": [],
                    "invoice_documents": [],
                    "other_documents": [],
                    "total_msa_value": 0.0,
                    "total_po_value": 0.0,
                    "total_invoice_value": 0.0,
                    "expires_on": None,
                    "days_until_expiry": None,
                    "expiring_soon": False,
                },
            )

            category_lower = (doc.category or "").lower()
            if "agreement" in category_lower:
                bucket["msa_documents"].append(doc)
            elif "po" in category_lower:
                bucket["po_documents"].append(doc)
            elif "invoice" in category_lower:
                bucket["invoice_documents"].append(doc)
            else:
                bucket["other_documents"].append(doc)

        for bucket in buckets.values():
            bucket["total_msa_value"] = sum(doc.amount for doc in bucket["msa_documents"])
            bucket["total_po_value"] = sum(doc.amount for doc in bucket["po_documents"])
            bucket["total_invoice_value"] = sum(doc.amount for doc in bucket["invoice_documents"])

            due_dates = [
                doc.due_date
                for doc in bucket["msa_documents"]
                if getattr(doc, "due_date", None)
            ]
            if due_dates:
                expires_on = min(due_dates)
                bucket["expires_on"] = expires_on
                if expires_on:
                    delta = (expires_on - now).days
                    bucket["days_until_expiry"] = delta
                    bucket["expiring_soon"] = delta <= 60

        relevant_categories = {"Client PO", "Vendor PO", "Client Invoice", "Vendor Invoice"}
        unlinked_documents = [
            doc for doc in documents
            if doc.category in relevant_categories and not self._resolve_msa_number(doc)
        ]

        return {"buckets": list(buckets.values()), "unlinked_documents": unlinked_documents}

    MSA_PATTERN = re.compile(r"(MSA[\s#:\-]*\d{3,}(?:[-/]\d{2,})?)", re.IGNORECASE)

    def generate_unlinked_alerts(self) -> List[Dict[str, Any]]:
        unlinked_documents = self.get_unlinked_documents()
        alerts = []
        for doc in unlinked_documents:
            doc_type = "PO" if "PO" in (doc.category or "") else "Invoice"
            alerts.append({
                "id": f"msa-unlinked-{doc.id}",
                "title": f"{doc_type} missing MSA link",
                "description": f"{doc.category} '{doc.title}' is not linked to any MSA. Tag the correct agreement to maintain compliance.",
                "level": "warning",
                "timestamp": datetime.utcnow(),
                "acknowledged": False,
                "document_id": doc.id
            })
        return alerts

    def _normalize_msa_value(self, value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        cleaned = value.strip().upper().replace(" ", "").replace("_", "-")
        # Extract canonical substring like MSA-2025-001
        match = self.MSA_PATTERN.search(cleaned)
        if not match:
            generic = re.search(r"(\d{4}[-/]\d{3,})", cleaned)
            if generic:
                cleaned = generic.group(1)
            else:
                return None
        else:
            cleaned = match.group(1)
        if not cleaned.startswith("MSA"):
            cleaned = f"MSA-{cleaned}"
        return cleaned

    def _resolve_msa_number(self, document: Document) -> Optional[str]:
        """
        Determine the MSA number for a document. Uses persisted value when available and
        falls back to parsing other fields (title, linked_to, PO/Invoice numbers, file name).
        """

        candidates = [
            document.msa_number,
            document.po_number,
            document.invoice_number,
            document.title,
            document.linked_to,
            document.file_path,
        ]

        for candidate in candidates:
            if not candidate:
                continue
            normalized = self._normalize_msa_value(str(candidate))
            if normalized:
                return normalized

        return None

    def get_unlinked_documents(self) -> List[Document]:
        documents, _, _ = self._load_documents_with_metadata()
        relevant_categories = {"Client PO", "Vendor PO", "Client Invoice", "Vendor Invoice"}
        return [
            doc for doc in documents
            if doc.category in relevant_categories and not self._resolve_msa_number(doc)
        ]
    
    def update_document(self, document_id: str, document: DocumentUpdate) -> Optional[Document]:
        db_document = self.get_document(document_id)
        if not db_document:
            return None
        
        update_data = document.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_document, field, value)
        
        self.db.commit()
        self.db.refresh(db_document)
        return db_document
    
    def delete_document(self, document_id: str) -> bool:
        db_document = self.get_document(document_id)
        if not db_document:
            return False
        
        self.db.delete(db_document)
        self.db.commit()
        return True
    
    def get_dashboard_insights(self) -> DashboardInsights:
        linking_service = DocumentLinkingService(self.db)
        
        # Calculate Active Client POs (approved Client PO documents)
        active_client_pos = self.db.query(Document).filter(
            Document.category == "Client PO",
            Document.status == "Approved"
        ).count()
        
        # Calculate previous period (30 days ago) for comparison
        thirty_days_ago = datetime.utcnow() - timedelta(days=30)
        active_client_pos_prev = self.db.query(Document).filter(
            Document.category == "Client PO",
            Document.status == "Approved",
            Document.created_at <= thirty_days_ago
        ).count()
        
        # Calculate delta for Active Client POs
        po_delta = self._calculate_percentage_change(active_client_pos_prev, active_client_pos)
        
        # Calculate Invoice Utilization (PO consumption)
        all_pos = self.db.query(Document).filter(
            Document.category.in_(["Client PO", "Vendor PO"])
        ).all()
        
        total_po_amount = sum(po.amount for po in all_pos)
        total_invoiced = 0.0
        
        for po in all_pos:
            consumption = linking_service.calculate_po_consumption(po)
            total_invoiced += consumption["total_invoiced"]
        
        invoice_utilization = (total_invoiced / total_po_amount * 100) if total_po_amount > 0 else 0
        
        # Calculate previous period utilization
        pos_prev = self.db.query(Document).filter(
            Document.category.in_(["Client PO", "Vendor PO"]),
            Document.created_at <= thirty_days_ago
        ).all()
        
        total_po_amount_prev = sum(po.amount for po in pos_prev) if pos_prev else 0
        total_invoiced_prev = 0.0
        
        for po in pos_prev:
            consumption = linking_service.calculate_po_consumption(po)
            total_invoiced_prev += consumption["total_invoiced"]
        
        utilization_prev = (total_invoiced_prev / total_po_amount_prev * 100) if total_po_amount_prev > 0 else 0
        utilization_delta = self._calculate_percentage_change(utilization_prev, invoice_utilization)
        
        # Get exceptions count
        exceptions_count = self.db.query(Exception).filter(Exception.resolved == False).count()
        exceptions_prev = self.db.query(Exception).filter(
            Exception.resolved == False,
            Exception.raised_at <= thirty_days_ago
        ).count()
        exceptions_delta = exceptions_count - exceptions_prev
        
        # Calculate average processing time from processed documents
        avg_processing_time = self._calculate_avg_processing_time()
        avg_processing_time_prev = self._calculate_avg_processing_time(thirty_days_ago)
        processing_time_delta = avg_processing_time - avg_processing_time_prev if avg_processing_time_prev > 0 else 0
        
        kpis = [
            KPIMetric(
                label="Active Client POs", 
                value=str(active_client_pos), 
                delta=f"{po_delta:+.1f}%", 
                helper="vs last 30 days"
            ),
            KPIMetric(
                label="Invoice Utilization", 
                value=f"{invoice_utilization:.0f}%", 
                delta=f"{utilization_delta:+.1f}%", 
                helper="PO caps consumed"
            ),
            KPIMetric(
                label="Exceptions", 
                value=str(exceptions_count), 
                delta=f"{exceptions_delta:+d} cases", 
                helper="open validation issues"
            ),
            KPIMetric(
                label="Avg. Processing Time", 
                value=f"{avg_processing_time:.0f}m", 
                delta=f"{processing_time_delta:+.0f}m", 
                helper="from ingest to validation"
            )
        ]
        
        # Get real utilization trend from last 6 months
        utilization_trend = self._calculate_utilization_trend()
        
        # Get category split
        category_counts = self.db.query(
            Document.category, 
            func.count(Document.id)
        ).group_by(Document.category).all()
        
        colors = ["#38bdf8", "#0ea5e9", "#6366f1", "#a855f7", "#f97316"]
        category_split = []
        for i, (category, count) in enumerate(category_counts):
            category_split.append(CategorySplit(
                name=category,
                value=count,
                fill=colors[i % len(colors)]
            ))
        
        # Get recent alerts and exceptions
        alerts = self.db.query(Alert).order_by(Alert.timestamp.desc()).limit(10).all()
        alerts = self.generate_unlinked_alerts() + alerts
        exceptions = self.db.query(Exception).order_by(Exception.raised_at.desc()).limit(10).all()
        
        return DashboardInsights(
            kpis=kpis,
            utilizationTrend=utilization_trend,
            categorySplit=category_split,
            alerts=alerts,
            exceptions=exceptions
        )
    
    def _calculate_percentage_change(self, old_value: float, new_value: float) -> float:
        """Calculate percentage change between two values"""
        if old_value == 0:
            return 100.0 if new_value > 0 else 0.0
        return ((new_value - old_value) / old_value) * 100
    
    def _calculate_avg_processing_time(self, before_date: Optional[datetime] = None) -> float:
        """Calculate average processing time from processed documents JSON files"""
        processed_dir = "./processed"
        if not os.path.exists(processed_dir):
            return 0.0
        
        processing_times = []
        
        for filename in os.listdir(processed_dir):
            if not filename.endswith('.json'):
                continue
            
            file_path = os.path.join(processed_dir, filename)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    processing_time_str = data.get('processing_time', '')
                    
                    if processing_time_str:
                        # Parse ISO format datetime
                        try:
                            processing_time = datetime.fromisoformat(processing_time_str.replace('Z', '+00:00'))
                            if before_date is None or processing_time <= before_date:
                                # For now, we'll estimate processing time as 2-5 minutes
                                # In a real system, you'd track actual processing duration
                                processing_times.append(3.0)  # Default estimate
                        except:
                            pass
            except:
                continue
        
        if not processing_times:
            return 0.0
        
        return sum(processing_times) / len(processing_times)
    
    def _calculate_utilization_trend(self) -> List[UtilizationTrend]:
        """
        Calculate monthly document activity trend from last 6 months.
        Shows total document amounts created per month, separated by Client and Vendor documents.
        This is more useful than showing only linked invoices (which may not exist).
        """
        now = datetime.utcnow()
        trend = []
        
        # Get last 6 months
        for i in range(5, -1, -1):  # 5 months ago to current month
            month_start = datetime(now.year, now.month, 1) - timedelta(days=30 * i)
            month_end = month_start + timedelta(days=30)
            
            # Get Client documents (Client PO + Client Invoice) created in this month
            client_docs = self.db.query(Document).filter(
                Document.category.in_(["Client PO", "Client Invoice"]),
                Document.created_at >= month_start,
                Document.created_at < month_end
            ).all()
            
            # Get Vendor documents (Vendor PO + Vendor Invoice) created in this month
            vendor_docs = self.db.query(Document).filter(
                Document.category.in_(["Vendor PO", "Vendor Invoice"]),
                Document.created_at >= month_start,
                Document.created_at < month_end
            ).all()
            
            # Sum amounts for the month (in thousands for display)
            client_monthly = sum(doc.amount for doc in client_docs) / 1000
            vendor_monthly = sum(doc.amount for doc in vendor_docs) / 1000
            
            # Month abbreviation
            month_name = month_start.strftime("%b")
            
            trend.append(UtilizationTrend(
                month=month_name,
                client=int(client_monthly) if client_monthly > 0 else 0,
                vendor=int(vendor_monthly) if vendor_monthly > 0 else 0
            ))
        
        # If no data at all, return empty trend with month names
        if not any(t.client > 0 or t.vendor > 0 for t in trend):
            return trend  # Return empty trend (all zeros)
        
        return trend
