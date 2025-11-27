import os
import uuid
import asyncio
import re
from typing import List, Set
from datetime import datetime
from fastapi import UploadFile
from sqlalchemy.orm import Session
from app.config import settings
from app.schemas import UploadedFile, UploadResponse, DocumentCreate
from app.services.pdf_processor import PDFProcessor
from app.services.alert_generator import AlertGenerator

class UploadService:
    # Class-level set to track files currently being processed (shared across instances)
    _processing_files: Set[str] = set()
    
    def __init__(self):
        self.upload_dir = settings.upload_dir
        self.max_file_size = settings.max_file_size
        self.pdf_processor = PDFProcessor()
    
    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename to prevent path traversal and other security issues"""
        # Remove any path components (directories)
        filename = os.path.basename(filename)
        
        # Remove or replace dangerous characters
        # Keep alphanumeric, spaces, dots, hyphens, underscores, and parentheses
        filename = re.sub(r'[^\w\s\.\-\(\)]', '_', filename)
        
        # Remove leading/trailing spaces and dots
        filename = filename.strip('. ')
        
        # Ensure filename is not empty
        if not filename:
            filename = "uploaded_file"
        
        return filename
    
    def _get_unique_filename(self, filename: str) -> str:
        """Get a unique filename, adding a number suffix if file already exists"""
        sanitized = self._sanitize_filename(filename)
        file_path = os.path.join(self.upload_dir, sanitized)
        
        # If file doesn't exist, use the original name
        if not os.path.exists(file_path):
            return sanitized
        
        # If file exists, add a number suffix
        name, ext = os.path.splitext(sanitized)
        counter = 1
        while True:
            new_filename = f"{name}_{counter}{ext}"
            new_file_path = os.path.join(self.upload_dir, new_filename)
            if not os.path.exists(new_file_path):
                return new_filename
            counter += 1
            # Safety limit to prevent infinite loop
            if counter > 1000:
                # Fallback to UUID if we can't find a unique name
                return f"{name}_{uuid.uuid4().hex[:8]}{ext}"
    
    async def upload_files(self, files: List[UploadFile]) -> UploadResponse:
        uploads = []
        
        for file in files:
            if not file.filename:
                continue
            
            # Use original filename (sanitized and made unique if needed)
            original_filename = file.filename
            stored_filename = self._get_unique_filename(original_filename)
            file_path = os.path.join(self.upload_dir, stored_filename)
            
            try:
                # Check file size
                file_size = 0
                content = await file.read()
                file_size = len(content)
                
                if file_size > self.max_file_size:
                    uploads.append(UploadedFile(
                        name=file.filename,
                        size=file_size,
                        type=file.content_type or "application/octet-stream",
                        status="rejected",
                        location=""
                    ))
                    continue
                
                # Save file
                with open(file_path, "wb") as buffer:
                    buffer.write(content)
                
                uploads.append(UploadedFile(
                    name=original_filename,  # Return original name to user
                    size=file_size,
                    type=file.content_type or "application/octet-stream",
                    status="queued",
                    location=f"/uploads/{stored_filename}"  # Use stored filename in location
                ))
                
            except Exception as e:
                uploads.append(UploadedFile(
                    name=file.filename,
                    size=0,
                    type=file.content_type or "application/octet-stream",
                    status="error",
                    location=""
                ))
        
        return UploadResponse(uploads=uploads)
    
    async def process_uploaded_pdf(self, filename: str, db: Session = None) -> dict:
        """Process an uploaded PDF file and extract data"""
        # First, try the exact filename
        file_path = self.get_file_path(filename)
        
        # If file doesn't exist, try to find it by sanitizing the filename
        # (in case the file was saved with a sanitized name)
        if not os.path.exists(file_path):
            # Try sanitized version
            sanitized = self._sanitize_filename(filename)
            sanitized_path = self.get_file_path(sanitized)
            if os.path.exists(sanitized_path):
                file_path = sanitized_path
                filename = sanitized
                print(f"✅ Found file with sanitized name: {sanitized}")
            else:
                # Try to find any file that matches (case-insensitive, ignoring special chars)
                # List all files in upload directory and find a match
                if os.path.exists(self.upload_dir):
                    for existing_file in os.listdir(self.upload_dir):
                        # Compare base names (without extension differences)
                        existing_base = os.path.splitext(existing_file)[0].lower()
                        target_base = os.path.splitext(sanitized)[0].lower()
                        # Remove common variations
                        existing_clean = existing_base.replace('_', '').replace('-', '').replace(' ', '')
                        target_clean = target_base.replace('_', '').replace('-', '').replace(' ', '')
                        if existing_clean == target_clean or existing_base == target_base:
                            file_path = self.get_file_path(existing_file)
                            filename = existing_file
                            print(f"✅ Found matching file: {existing_file}")
                            break
        
        print(f"🔍 Processing file: {filename}")
        print(f"🔍 File path: {file_path}")
        print(f"🔍 File exists: {os.path.exists(file_path)}")
        print(f"🔍 Upload directory exists: {os.path.exists(self.upload_dir)}")
        if os.path.exists(self.upload_dir):
            print(f"🔍 Files in upload directory: {os.listdir(self.upload_dir)}")
        
        if not os.path.exists(file_path):
            error_msg = f"File '{filename}' not found at path '{file_path}'. Please upload the file first."
            print(f"❌ {error_msg}")
            return {
                "success": False,
                "error": error_msg,
                "error_type": "file_not_found"
            }
            return {"success": False, "error": "File not found"}
        
        # Check if it's a PDF
        if not filename.lower().endswith('.pdf'):
            return {"success": False, "error": "File is not a PDF"}
        
        # Check if file is already being processed (prevent duplicate processing)
        if filename in self._processing_files:
            return {
                "success": False, 
                "error": f"File '{filename}' is already being processed. Please wait.",
                "already_processing": True
            }
        
        # Check if document already exists in database (by filename)
        # We'll do a preliminary check here, and a more thorough check after processing
        if db:
            existing_doc = self._check_document_exists_in_db(filename, db)
            if existing_doc:
                print(f"✅ Document '{filename}' already exists in database - skipping processing")
                # Return existing document data
                return {
                    "success": True,
                    "already_exists": True,
                    "message": f"Document '{filename}' has already been processed.",
                    "document_id": existing_doc.id,
                    "document_db_id": existing_doc.id,
                    "extracted_data": {
                        "title": existing_doc.title,
                        "client": existing_doc.client,
                        "vendor": existing_doc.vendor,
                        "amount": existing_doc.amount,
                        "currency": existing_doc.currency,
                        "date": existing_doc.created_at.isoformat() if existing_doc.created_at else None,
                        "due_date": existing_doc.due_date.isoformat() if existing_doc.due_date else None,
                        "po_number": existing_doc.po_number,
                        "invoice_number": existing_doc.invoice_number,
                    },
                    "document_type": existing_doc.category,
                    "confidence": existing_doc.confidence,
                    "processing_time": existing_doc.created_at.isoformat() if existing_doc.created_at else datetime.now().isoformat()
                }
        
        # Check if file already exists in S3 (before processing)
        existing_s3_key = self.pdf_processor._check_file_exists_in_s3(filename)
        if existing_s3_key:
            print(f"⚠️  File '{filename}' already exists in S3 at: {existing_s3_key}")
            # Try to get existing processed data
            existing_data = self.pdf_processor._get_existing_processed_data(filename, existing_s3_key)
            if existing_data:
                print(f"✅ Found existing processed data - returning cached result")
                return existing_data
            # If file exists in S3 but no processed data, check database one more time
            if db:
                existing_doc = self._check_document_exists_in_db(filename, db)
                if existing_doc:
                    print(f"✅ Document exists in database - skipping re-processing")
                    return {
                        "success": True,
                        "already_exists": True,
                        "message": f"Document '{filename}' has already been processed.",
                        "document_id": existing_doc.id,
                        "document_db_id": existing_doc.id,
                        "extracted_data": {
                            "title": existing_doc.title,
                            "client": existing_doc.client,
                            "vendor": existing_doc.vendor,
                            "amount": existing_doc.amount,
                            "currency": existing_doc.currency,
                            "date": existing_doc.created_at.isoformat() if existing_doc.created_at else None,
                            "due_date": existing_doc.due_date.isoformat() if existing_doc.due_date else None,
                            "po_number": existing_doc.po_number,
                            "invoice_number": existing_doc.invoice_number,
                        },
                        "document_type": existing_doc.category,
                        "confidence": existing_doc.confidence,
                        "processing_time": existing_doc.created_at.isoformat() if existing_doc.created_at else datetime.now().isoformat()
                    }
            # File exists in S3 but no processed data AND not in database
            # Allow re-processing by downloading from S3 and processing
            print(f"⚠️  File exists in S3 but no processed data found. Will re-process using S3 file.")
            # Continue to processing - the PDFProcessor will use the existing S3 file
        
        # Mark file as being processed
        self._processing_files.add(filename)
        
        try:
            # Process the PDF (will upload to S3 and process with Textract)
            result = await self.pdf_processor.process_pdf(file_path)
            
            # Check if processing was skipped due to existing file
            if result.get("already_exists", False):
                return result
            
            # Save the processed document if successful
            if result.get("success", False):
                self.save_processed_document(result)
                
                # Save to database and generate alerts if db session is provided
                if db:
                    try:
                        # Check again if document exists using extracted data (more thorough check)
                        extracted_data = result.get("extracted_data", {})
                        existing_doc = self._check_document_exists_in_db(filename, db, extracted_data)
                        if existing_doc:
                            print(f"⚠️  Document already exists in database (ID: {existing_doc.id}) - skipping save")
                            result["already_exists"] = True
                            result["document_db_id"] = existing_doc.id
                            return result
                        
                        document = self._save_to_database(result, filename, db)
                        if document:
                            # Generate alerts for the new document
                            alert_generator = AlertGenerator(db)
                            alerts = alert_generator.generate_alerts_for_document(document)
                            db.commit()
                            result["alerts_generated"] = len(alerts)
                            result["document_db_id"] = document.id
                    except Exception as e:
                        print(f"Error saving to database or generating alerts: {e}")
                        # Don't fail the whole process if DB save fails
                        if db:
                            db.rollback()
            
            return result
        finally:
            # Remove from processing set after completion (with a small delay to prevent rapid re-processing)
            await asyncio.sleep(2)  # Wait 2 seconds before allowing re-processing
            self._processing_files.discard(filename)
    
    def _save_to_database(self, result: dict, filename: str, db: Session):
        """Save processed document to database"""
        from app.models import Document
        from app.services.document_service import DocumentService
        
        extracted_data = result.get("extracted_data", {})
        document_type = result.get("document_type", "Unknown")
        confidence = result.get("confidence", 0.0)
        document_id = result.get("document_id", str(uuid.uuid4()))
        
        # Parse due date if available
        due_date = None
        if extracted_data.get("due_date"):
            try:
                # Try to parse the date string
                due_date_str = extracted_data["due_date"]
                # Handle various date formats
                if isinstance(due_date_str, str):
                    # Try common formats
                    for fmt in ["%Y-%m-%d", "%d %b %Y", "%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S"]:
                        try:
                            due_date = datetime.strptime(due_date_str.split()[0], fmt)
                            break
                        except:
                            continue
            except:
                pass
        
        # Create document
        document_service = DocumentService(db)
        document_create = DocumentCreate(
            title=extracted_data.get("title", filename),
            category=document_type,
            client=extracted_data.get("client", "Unknown Client"),
            vendor=extracted_data.get("vendor"),
            amount=float(extracted_data.get("amount", 0)),
            currency=extracted_data.get("currency", "USD"),
            status="Draft",
            due_date=due_date,
            confidence=confidence,
            pdf_url=f"/api/uploads/{filename}",
            file_path=filename,
            processed=True,
            po_number=extracted_data.get("po_number"),
            invoice_number=extracted_data.get("invoice_number")
        )
        
        # Use the enhanced duplicate check method
        existing_doc = self._check_document_exists_in_db(filename, db, extracted_data)
        
        if existing_doc:
            # Document already exists - return it without updating
            print(f"⚠️  Document already exists in database (ID: {existing_doc.id}, file_path: {existing_doc.file_path}) - skipping save")
            return existing_doc
        
        # Create new document with the extracted document_id
        db_document = Document(
            id=document_id,
            **document_create.dict()
        )
        db.add(db_document)
        db.commit()
        db.refresh(db_document)
        return db_document
    
    def get_file_path(self, filename: str) -> str:
        return os.path.join(self.upload_dir, filename)
    
    def _check_document_exists_in_db(self, filename: str, db: Session, extracted_data: dict = None):
        """Check if a document with this filename or similar data already exists in the database"""
        from app.models import Document
        
        # Check by file_path (filename) - primary check
        existing = db.query(Document).filter(
            Document.file_path == filename
        ).first()
        
        if existing:
            return existing
        
        # Also check by original filename (in case file_path was modified)
        # Extract base filename without extension for comparison
        base_name = os.path.splitext(filename)[0]
        # Check if any document's file_path contains this base name
        existing = db.query(Document).filter(
            Document.file_path.isnot(None),
            Document.file_path.contains(base_name)
        ).first()
        
        if existing:
            return existing
        
        # If we have extracted data, check by invoice number or PO number
        if extracted_data:
            # Check by invoice number (for invoices)
            invoice_number = extracted_data.get("invoice_number")
            if invoice_number:
                existing = db.query(Document).filter(
                    Document.invoice_number == invoice_number,
                    Document.category.in_(["Client Invoice", "Vendor Invoice"])
                ).first()
                if existing:
                    return existing
            
            # Check by PO number (for POs)
            po_number = extracted_data.get("po_number")
            if po_number:
                existing = db.query(Document).filter(
                    Document.po_number == po_number,
                    Document.category.in_(["Client PO", "Vendor PO"])
                ).first()
                if existing:
                    return existing
            
            # Check by title + amount + client (for documents without invoice/PO numbers)
            title = extracted_data.get("title")
            amount = extracted_data.get("amount")
            client = extracted_data.get("client")
            
            if title and amount:
                # Match by title and amount (within 1% tolerance)
                # This catches duplicates even if client name extraction varies slightly
                amount_tolerance = amount * 0.01  # 1% tolerance
                existing = db.query(Document).filter(
                    Document.title == title,
                    Document.amount >= amount - amount_tolerance,
                    Document.amount <= amount + amount_tolerance
                ).first()
                if existing:
                    return existing
                
                # Also check by amount and client if client is available
                if client and client != "Unknown Client":
                    existing = db.query(Document).filter(
                        Document.client == client,
                        Document.amount >= amount - amount_tolerance,
                        Document.amount <= amount + amount_tolerance
                    ).first()
                    if existing:
                        return existing
        
        return None
    
    def delete_file(self, filename: str) -> bool:
        try:
            file_path = self.get_file_path(filename)
            if os.path.exists(file_path):
                os.remove(file_path)
                return True
            return False
        except Exception:
            return False
    
    def save_processed_document(self, result: dict) -> bool:
        """Save processed document result to JSON file"""
        try:
            import json
            processed_dir = "./processed"
            os.makedirs(processed_dir, exist_ok=True)
            
            # Create filename from document ID
            document_id = result.get('document_id', 'unknown')
            filename = f"{document_id}.json"
            file_path = os.path.join(processed_dir, filename)
            
            # Save the result
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            
            return True
        except Exception as e:
            print(f"Error saving processed document: {e}")
            return False
