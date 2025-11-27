import os
import re
import uuid
import time
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from app.config import settings

class PDFProcessor:
    def __init__(self):
        self.upload_dir = "./uploads"
        self.processed_dir = "./processed"
        self.categories = [
            "Client PO", "Vendor PO", "Client Invoice", 
            "Vendor Invoice", "Service Agreement"
        ]
        
        # Create directories if they don't exist
        os.makedirs(self.upload_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
        
        # Initialize AWS clients
        self.textract_client = None
        self.s3_client = None
        self._initialize_aws_clients()
    
    def _initialize_aws_clients(self):
        """Initialize AWS Textract and S3 clients with credentials from settings"""
        try:
            aws_config = {}
            if settings.aws_access_key_id and settings.aws_secret_access_key:
                aws_config = {
                    'aws_access_key_id': settings.aws_access_key_id,
                    'aws_secret_access_key': settings.aws_secret_access_key,
                    'region_name': settings.aws_region
                }
            else:
                # Try to use default credentials (from environment or IAM role)
                aws_config = {'region_name': settings.aws_region}
            
            self.textract_client = boto3.client('textract', **aws_config)
            self.s3_client = boto3.client('s3', **aws_config)
            
        except Exception as e:
            print(f"Warning: Failed to initialize AWS clients: {e}")
            print("Textract will not be available. Please configure AWS credentials.")
    
    async def process_pdf(self, file_path: str) -> Dict:
        """Process a PDF file and extract structured data using Amazon Textract with S3-based async processing"""
        temp_s3_key = None
        try:
            if not self.textract_client or not self.s3_client:
                error_msg = "AWS clients not initialized. Please configure AWS credentials and S3 bucket."
                print(f"❌ {error_msg}")
                print(f"   AWS_ACCESS_KEY_ID set: {bool(settings.aws_access_key_id)}")
                print(f"   AWS_SECRET_ACCESS_KEY set: {bool(settings.aws_secret_access_key)}")
                print(f"   AWS_REGION: {settings.aws_region}")
                return {
                    "success": False,
                    "error": error_msg,
                    "error_type": "aws_not_configured",
                    "processing_time": datetime.now().isoformat()
                }
            
            if not settings.aws_s3_bucket:
                error_msg = "S3 bucket not configured. Please set AWS_S3_BUCKET in environment variables."
                print(f"❌ {error_msg}")
                print(f"   Current AWS_S3_BUCKET value: '{settings.aws_s3_bucket}' (empty or None)")
                print(f"   Check Railway Variables tab and ensure AWS_S3_BUCKET is set correctly")
                return {
                    "success": False,
                    "error": error_msg,
                    "error_type": "s3_bucket_not_configured",
                    "processing_time": datetime.now().isoformat()
                }
            
            # Get filename
            filename = os.path.basename(file_path)
            
            # Check if file already exists in S3 (in any organized folder)
            existing_s3_key = self._check_file_exists_in_s3(filename)
            if existing_s3_key:
                print(f"⚠️  File '{filename}' already exists in S3 at: {existing_s3_key}")
                # Try to get existing processed data from local storage
                existing_data = self._get_existing_processed_data(filename, existing_s3_key)
                if existing_data:
                    print(f"✅ Found existing processed data - returning cached result")
                    return existing_data
                # File exists in S3 - don't re-upload or re-process
                return {
                    "success": False,
                    "error": f"File '{filename}' already exists in S3. Processing skipped to avoid duplicates.",
                    "already_exists": True,
                    "processing_time": datetime.now().isoformat()
                }
            
            # Read PDF file
            with open(file_path, 'rb') as document:
                pdf_bytes = document.read()
            
            # Validate PDF file size (S3-based processing supports up to 500MB)
            if len(pdf_bytes) > 500 * 1024 * 1024:  # 500MB limit
                return {
                    "success": False,
                    "error": f"PDF file is too large ({len(pdf_bytes) / 1024 / 1024:.2f}MB). Maximum size is 500MB.",
                    "processing_time": datetime.now().isoformat()
                }
            
            # Generate unique S3 key for this PDF (temporary location for processing)
            temp_s3_key = f"textract-processing/{uuid.uuid4()}/{filename}"
            
            # Upload PDF to S3 (temporary location) - only if file doesn't exist
            try:
                self.s3_client.upload_file(
                    file_path,
                    settings.aws_s3_bucket,
                    temp_s3_key,
                    ExtraArgs={'ContentType': 'application/pdf'}
                )
                print(f"📤 Uploaded to S3: s3://{settings.aws_s3_bucket}/{temp_s3_key}")
            except Exception as e:
                return {
                    "success": False,
                    "error": f"Failed to upload PDF to S3: {str(e)}",
                    "processing_time": datetime.now().isoformat()
                }
            
            # Start async Textract job
            try:
                response = self.textract_client.start_document_text_detection(
                    DocumentLocation={
                        'S3Object': {
                            'Bucket': settings.aws_s3_bucket,
                            'Name': temp_s3_key
                        }
                    }
                )
                job_id = response['JobId']
                print(f"🔄 Started Textract job: {job_id}")
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                error_message = e.response.get('Error', {}).get('Message', str(e))
                # Clean up S3 file
                self._cleanup_s3_file(temp_s3_key)
                return {
                    "success": False,
                    "error": f"AWS Textract error ({error_code}): {error_message}",
                    "processing_time": datetime.now().isoformat()
                }
            
            # Poll for job completion
            text_content = self._wait_for_textract_job(job_id)
            if not text_content:
                # Clean up S3 file
                self._cleanup_s3_file(temp_s3_key)
                return {
                    "success": False,
                    "error": "Textract job failed or timed out",
                    "processing_time": datetime.now().isoformat()
                }
            
            # Classify document type
            document_type = self._classify_document(text_content)
            
            # Move file to organized folder based on document type
            permanent_s3_key = self._organize_file_in_s3(temp_s3_key, filename, document_type)
            if permanent_s3_key:
                print(f"📁 Organized file in S3: s3://{settings.aws_s3_bucket}/{permanent_s3_key}")
                # Delete temporary file
                self._cleanup_s3_file(temp_s3_key)
            else:
                # If organization failed, just clean up temp file
                self._cleanup_s3_file(temp_s3_key)
            
            # Extract structured data
            extracted_data = self._extract_structured_data(text_content, document_type, "")
            
            # Calculate confidence score
            confidence = self._calculate_confidence(text_content, document_type)
            
            # Generate document ID
            document_id = self._generate_document_id(document_type)
            
            return {
                "success": True,
                "document_id": document_id,
                "document_type": document_type,
                "confidence": confidence,
                "extracted_data": extracted_data,
                "full_text": text_content,
                "processing_time": datetime.now().isoformat()
            }
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_message = e.response.get('Error', {}).get('Message', str(e))
            if temp_s3_key:
                self._cleanup_s3_file(temp_s3_key)
            return {
                "success": False,
                "error": f"AWS Textract error ({error_code}): {error_message}",
                "processing_time": datetime.now().isoformat()
            }
        except Exception as e:
            if temp_s3_key:
                self._cleanup_s3_file(temp_s3_key)
            return {
                "success": False,
                "error": str(e),
                "processing_time": datetime.now().isoformat()
            }
    
    def _wait_for_textract_job(self, job_id: str, max_wait_time: int = 300) -> Optional[str]:
        """Wait for Textract job to complete and return extracted text"""
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            try:
                response = self.textract_client.get_document_text_detection(JobId=job_id)
                status = response['JobStatus']
                
                if status == 'SUCCEEDED':
                    # Extract text from blocks
                    text_lines = []
                    next_token = None
                    
                    while True:
                        for block in response.get('Blocks', []):
                            if block['BlockType'] == 'LINE':
                                text_lines.append(block.get('Text', ''))
                        
                        # Check if there are more pages
                        next_token = response.get('NextToken')
                        if not next_token:
                            break
                        
                        response = self.textract_client.get_document_text_detection(
                            JobId=job_id,
                            NextToken=next_token
                        )
                    
                    return '\n'.join(text_lines)
                
                elif status == 'FAILED':
                    error_message = response.get('StatusMessage', 'Unknown error')
                    print(f"Textract job failed: {error_message}")
                    return None
                
                elif status in ['IN_PROGRESS', 'PARTIAL_SUCCESS']:
                    # Wait before checking again
                    time.sleep(2)
                    continue
                
            except ClientError as e:
                print(f"Error checking Textract job status: {e}")
                return None
        
        print(f"Textract job timed out after {max_wait_time} seconds")
        return None
    
    def _check_file_exists_in_s3(self, filename: str) -> Optional[str]:
        """Check if a file with the given filename already exists in any S3 folder"""
        if not self.s3_client or not settings.aws_s3_bucket:
            return None
        
        try:
            # List of folders to check (matching the folder mapping in _organize_file_in_s3)
            folders_to_check = [
                "Purchase Order/",
                "Invoices/",
                "Service Agreements/",
                "Contract/",
            ]
            
            # Check each folder for the filename
            for folder in folders_to_check:
                s3_key = f"{folder}{filename}"
                try:
                    # Try to head the object (fast check without downloading)
                    self.s3_client.head_object(
                        Bucket=settings.aws_s3_bucket,
                        Key=s3_key
                    )
                    # File exists!
                    return s3_key
                except ClientError as e:
                    error_code = e.response.get('Error', {}).get('Code', '')
                    if error_code == '404':
                        # File doesn't exist in this folder, continue checking
                        continue
                    else:
                        # Some other error, log it but continue
                        print(f"Warning: Error checking S3 for {s3_key}: {e}")
                        continue
            
            # File not found in any folder
            return None
            
        except Exception as e:
            print(f"Warning: Error checking if file exists in S3: {e}")
            return None
    
    def _get_existing_processed_data(self, filename: str, s3_key: str = None) -> Optional[Dict]:
        """Try to retrieve existing processed data for a file from local storage
        Matches by checking if the processed data references the same S3 location or filename
        """
        try:
            # Check if processed JSON file exists
            processed_dir = self.processed_dir
            if not os.path.exists(processed_dir):
                return None
            
            # Look for JSON files that might contain this filename
            for json_file in os.listdir(processed_dir):
                if json_file.endswith('.json'):
                    json_path = os.path.join(processed_dir, json_file)
                    try:
                        import json
                        with open(json_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            
                            # Check if this processed data is for the same file
                            if not data.get('success', False):
                                continue
                            
                            # Try to match by checking extracted_data for filename patterns
                            # or by checking if the document was processed from the same S3 location
                            extracted_data = data.get('extracted_data', {})
                            
                            # Check if we can match by filename in the extracted data
                            # Some documents might have the filename in title or other fields
                            title = extracted_data.get('title', '').lower()
                            if filename.lower() in title or title in filename.lower():
                                return data
                            
                            # If S3 key is provided, we could also check metadata
                            # For now, we'll do a simple check: if the document_id format matches
                            # the expected pattern for this filename, it might be a match
                            # But this is not perfect, so we'll be conservative
                            
                    except Exception as e:
                        print(f"Warning: Error reading processed file {json_file}: {e}")
                        continue
            
            # No matching processed data found
            return None
            
        except Exception as e:
            print(f"Warning: Error getting existing processed data: {e}")
            return None
    
    def _organize_file_in_s3(self, temp_s3_key: str, filename: str, document_type: str) -> Optional[str]:
        """Move file from temporary location to organized folder based on document type"""
        try:
            # Map document types to S3 folders
            folder_mapping = {
                "Client PO": "Purchase Order/",
                "Vendor PO": "Purchase Order/",
                "Client Invoice": "Invoices/",
                "Vendor Invoice": "Invoices/",
                "Service Agreement": "Service Agreements/",
                "Unknown": "Contract/"  # Default folder for unknown types
            }
            
            # Get target folder
            target_folder = folder_mapping.get(document_type, "Contract/")
            permanent_s3_key = f"{target_folder}{filename}"
            
            # Copy file to permanent location
            copy_source = {
                'Bucket': settings.aws_s3_bucket,
                'Key': temp_s3_key
            }
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=settings.aws_s3_bucket,
                Key=permanent_s3_key,
                MetadataDirective='COPY'
            )
            
            return permanent_s3_key
            
        except Exception as e:
            print(f"Warning: Failed to organize file in S3: {e}")
            return None
    
    def _cleanup_s3_file(self, s3_key: str):
        """Delete file from S3 after processing"""
        try:
            if self.s3_client and settings.aws_s3_bucket:
                self.s3_client.delete_object(Bucket=settings.aws_s3_bucket, Key=s3_key)
                print(f"🗑️  Deleted temporary S3 file: {s3_key}")
        except Exception as e:
            print(f"Warning: Failed to delete S3 file {s3_key}: {e}")
    
    def _extract_text_with_textract(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF using Textract detect_document_text"""
        try:
            # Validate PDF bytes before sending
            if not pdf_bytes or len(pdf_bytes) == 0:
                raise ValueError("PDF bytes are empty")
            
            if not pdf_bytes.startswith(b'%PDF'):
                raise ValueError("Invalid PDF format: missing PDF header")
            
            # Ensure we have a valid PDF structure
            if b'%%EOF' not in pdf_bytes[-1000:]:
                raise ValueError("Invalid PDF format: missing EOF marker")
            
            # Call Textract with proper error handling
            response = self.textract_client.detect_document_text(
                Document={'Bytes': pdf_bytes}
            )
            
            # Extract text from blocks
            text_lines = []
            for block in response.get('Blocks', []):
                if block['BlockType'] == 'LINE':
                    text_lines.append(block.get('Text', ''))
            
            return '\n'.join(text_lines)
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_message = e.response.get('Error', {}).get('Message', str(e))
            print(f"Textract ClientError ({error_code}): {error_message}")
            # Re-raise to be handled by the caller
            raise
        except Exception as e:
            print(f"Error extracting text with Textract: {type(e).__name__}: {e}")
            raise
    
    def _analyze_document_with_textract(self, pdf_bytes: bytes) -> Tuple[str, str]:
        """Analyze document for forms and tables using Textract analyze_document"""
        forms_text = ""
        tables_text = ""
        
        try:
            # Use analyze_document with FORMS and TABLES feature types
            response = self.textract_client.analyze_document(
                Document={'Bytes': pdf_bytes},
                FeatureTypes=['FORMS', 'TABLES']
            )
            
            # Extract forms (key-value pairs)
            forms_data = {}
            for block in response.get('Blocks', []):
                if block['BlockType'] == 'KEY_VALUE_SET':
                    if 'KEY' in block.get('EntityTypes', []):
                        # This is a key
                        key_text = self._get_text_from_block(block, response.get('Blocks', []))
                        # Find corresponding value
                        value_text = self._get_value_for_key(block, response.get('Blocks', []))
                        if key_text and value_text:
                            forms_data[key_text] = value_text
            
            # Convert forms to text
            forms_text = '\n'.join([f"{k}: {v}" for k, v in forms_data.items()])
            
            # Extract tables
            tables = []
            for block in response.get('Blocks', []):
                if block['BlockType'] == 'TABLE':
                    table_text = self._extract_table_text(block, response.get('Blocks', []))
                    if table_text:
                        tables.append(table_text)
            
            tables_text = '\n\n'.join(tables)
            
        except ClientError as e:
            # If analyze_document fails (e.g., not supported in all regions or requires async),
            # fall back to detect_document_text only
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code != 'InvalidParameterException':
                print(f"Warning: analyze_document failed: {e}")
        
        return forms_text, tables_text
    
    def _get_text_from_block(self, block: Dict, all_blocks: List[Dict]) -> str:
        """Get text from a block by following relationships"""
        text_parts = []
        if 'Relationships' in block:
            for relationship in block['Relationships']:
                if relationship['Type'] == 'CHILD':
                    for child_id in relationship['Ids']:
                        child_block = next((b for b in all_blocks if b['Id'] == child_id), None)
                        if child_block and child_block['BlockType'] == 'WORD':
                            text_parts.append(child_block.get('Text', ''))
        return ' '.join(text_parts)
    
    def _get_value_for_key(self, key_block: Dict, all_blocks: List[Dict]) -> str:
        """Get the value associated with a key block"""
        if 'Relationships' not in key_block:
            return ""
        
        for relationship in key_block['Relationships']:
            if relationship['Type'] == 'VALUE':
                for value_id in relationship['Ids']:
                    value_block = next((b for b in all_blocks if b['Id'] == value_id), None)
                    if value_block:
                        return self._get_text_from_block(value_block, all_blocks)
        return ""
    
    def _extract_table_text(self, table_block: Dict, all_blocks: List[Dict]) -> str:
        """Extract text from a table block"""
        # This is a simplified table extraction
        # For production, you'd want to reconstruct the table structure properly
        text_parts = []
        if 'Relationships' in table_block:
            for relationship in table_block['Relationships']:
                if relationship['Type'] == 'CHILD':
                    for cell_id in relationship['Ids']:
                        cell_block = next((b for b in all_blocks if b['Id'] == cell_id), None)
                        if cell_block and cell_block['BlockType'] == 'CELL':
                            cell_text = self._get_text_from_block(cell_block, all_blocks)
                            if cell_text:
                                text_parts.append(cell_text)
        return ' | '.join(text_parts)
    
    def _classify_document(self, text: str) -> str:
        """Classify document type using keyword matching"""
        text_lower = text.lower()
        
        # Define keywords for each document type
        type_keywords = {
            "Client PO": ["purchase order", "client", "po", "order from"],
            "Vendor PO": ["purchase order", "vendor", "supplier", "order to"],
            "Client Invoice": ["invoice", "client", "bill to", "invoice to"],
            "Vendor Invoice": ["invoice", "vendor", "supplier", "invoice from"],
            "Service Agreement": ["agreement", "contract", "service", "terms and conditions"]
        }
        
        scores = {}
        for doc_type, keywords in type_keywords.items():
            score = sum(1 for keyword in keywords if keyword in text_lower)
            scores[doc_type] = score
        
        # Return the type with highest score, or "Unknown" if no match
        if scores and max(scores.values()) > 0:
            return max(scores, key=scores.get)
        
        return "Unknown"
    
    def _extract_structured_data(self, text: str, document_type: str, forms_data: str = "") -> Dict:
        """Extract structured data based on document type"""
        # Combine text and forms data for better extraction
        combined_text = f"{text}\n{forms_data}" if forms_data else text
        
        extracted = {
            "title": self._extract_title(combined_text),
            "client": self._extract_client(combined_text),
            "vendor": self._extract_vendor(combined_text),
            "amount": self._extract_amount(combined_text),
            "currency": self._extract_currency(combined_text),
            "date": self._extract_date(combined_text),
            "due_date": self._extract_due_date(combined_text),
            "po_number": self._extract_po_number(combined_text),
            "invoice_number": self._extract_invoice_number(combined_text),
            "vendor_address": self._extract_vendor_address(combined_text),
            "client_address": self._extract_client_address(combined_text),
            "summary": self._extract_summary(combined_text),
            "key_terms": self._extract_key_terms(combined_text),
            "contact_info": self._extract_contact_info(combined_text)
        }
        
        return extracted
    
    def _extract_title(self, text: str) -> str:
        """Extract document title"""
        clean_text = text.strip()
        
        # Look for common title patterns
        patterns = [
            r"Title:\s*(.+?)(?:\n|$)",
            r"Subject:\s*(.+?)(?:\n|$)",
            r"Document:\s*(.+?)(?:\n|$)",
            r"INVOICE\s+(.+?)(?:\n|$)",
            r"PURCHASE ORDER\s+(.+?)(?:\n|$)",
            r"AGREEMENT\s+(.+?)(?:\n|$)"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, clean_text, re.IGNORECASE)
            if match:
                title = match.group(1).strip()
                if len(title) > 5 and len(title) < 100:
                    return title
        
        # Look for company names or document headers
        company_patterns = [
            r"^([A-Z][A-Z\s&]+(?:LTD|LLC|INC|CORP|COMPANY))",
            r"^([A-Z][A-Z\s&]+(?:TECHNOLOGY|SERVICES|SOLUTIONS))",
            r"^([A-Z][A-Z\s&]+(?:DIGITAL|INFORMATION))"
        ]
        
        for pattern in company_patterns:
            match = re.search(pattern, clean_text, re.MULTILINE)
            if match:
                title = match.group(1).strip()
                if len(title) > 5 and len(title) < 80:
                    return title
        
        # Fallback: use first meaningful line
        lines = clean_text.split('\n')
        for line in lines[:10]:
            line = line.strip()
            if len(line) > 10 and len(line) < 100 and not line.startswith('Company Registration'):
                return line
        
        return "Document"
    
    def _extract_client(self, text: str) -> str:
        """Extract client name with improved patterns"""
        clean_text = text.strip()
        lines = clean_text.split('\n')
        
        # Look for "Bill To" and get the company name on the next line(s)
        for i, line in enumerate(lines):
            line_lower = line.lower().strip()
            if 'bill to' in line_lower or 'billed to' in line_lower:
                # Get the next non-empty line(s) which should be the client name
                client_parts = []
                for j in range(i + 1, min(i + 5, len(lines))):
                    next_line = lines[j].strip()
                    if next_line and len(next_line) > 2:
                        # Skip common labels
                        if next_line.lower() in ['shipped to', 'ship to', 'deliver to']:
                            continue
                        # Stop if we hit another label
                        if ':' in next_line and len(next_line.split(':')[0]) < 20:
                            break
                        client_parts.append(next_line)
                        # Stop if we have a complete company name (usually 1-2 lines)
                        if len(client_parts) >= 1 and any(char in next_line for char in ['(', ')', 'L.L.C', 'LLC', 'LTD', 'INC']):
                            break
                if client_parts:
                    client = ' '.join(client_parts).strip()
                    client = re.sub(r'\s+', ' ', client)
                    if len(client) > 3 and len(client) < 150:
                        return client
        
        # Enhanced patterns for client extraction (fallback)
        patterns = [
            r"Bill\s+To[:\s]*\n\s*([A-Z][^\n]{5,100}?)(?:\n|$)",
            r"Client:\s*(.+?)(?:\n|$)",
            r"Customer:\s*(.+?)(?:\n|$)",
            r"Bill\s+To[:\s]*(.+?)(?:\n|$)",
            r"Billed\s+To[:\s]*(.+?)(?:\n|$)",
        ]
        
        for pattern in patterns:
            match = re.search(pattern, clean_text, re.IGNORECASE | re.MULTILINE)
            if match:
                client = match.group(1).strip()
                # Skip if it's just a label
                if client.lower() in ['shipped to', 'ship to', 'deliver to']:
                    continue
                client = re.sub(r'\s+', ' ', client)
                if len(client) > 3 and len(client) < 150:
                    return client
        
        # Look for company names in document headers
        company_patterns = [
            r"^([A-Z][A-Z\s&]+(?:LTD|LLC|INC|CORP|COMPANY|LLP))",
            r"^([A-Z][A-Z\s&]+(?:TECHNOLOGY|SERVICES|SOLUTIONS|SYSTEMS))",
            r"^([A-Z][A-Z\s&]+(?:DIGITAL|INFORMATION|CONSULTING))"
        ]
        
        for pattern in company_patterns:
            match = re.search(pattern, clean_text, re.MULTILINE)
            if match:
                company = match.group(1).strip()
                if len(company) > 5 and len(company) < 80:
                    return company
        
        return "Unknown Client"
    
    def _extract_vendor(self, text: str) -> str:
        """Extract vendor name with improved patterns"""
        clean_text = text.strip()
        
        # Enhanced patterns for vendor extraction
        patterns = [
            r"Vendor:\s*(.+?)(?:\n|$)",
            r"Supplier:\s*(.+?)(?:\n|$)",
            r"From:\s*(.+?)(?:\n|$)",
            r"Vendor\s+Name[:\s]*(.+?)(?:\n|$)",
            r"Supplier\s+Name[:\s]*(.+?)(?:\n|$)",
            r"Company\s+Name[:\s]*(.+?)(?:\n|$)",
            r"Business\s+Name[:\s]*(.+?)(?:\n|$)"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, clean_text, re.IGNORECASE | re.MULTILINE)
            if match:
                vendor = match.group(1).strip()
                vendor = re.sub(r'\s+', ' ', vendor)
                if len(vendor) > 3 and len(vendor) < 100:
                    return vendor
        
        # Look for company names in document headers (for vendor invoices)
        company_patterns = [
            r"^([A-Z][A-Z\s&]+(?:LTD|LLC|INC|CORP|COMPANY|LLP))",
            r"^([A-Z][A-Z\s&]+(?:TECHNOLOGY|SERVICES|SOLUTIONS|SYSTEMS))",
            r"^([A-Z][A-Z\s&]+(?:DIGITAL|INFORMATION|CONSULTING))"
        ]
        
        for pattern in company_patterns:
            match = re.search(pattern, clean_text, re.MULTILINE)
            if match:
                company = match.group(1).strip()
                if len(company) > 5 and len(company) < 80:
                    return company
        
        return None
    
    def _extract_amount(self, text: str) -> float:
        """Extract monetary amount with improved patterns - prioritize totals over subtotals"""
        clean_text = text.strip()
        lines = clean_text.split('\n')
        
        # Priority patterns - look for invoice total, amount due, grand total first
        # Highest priority: Total including VAT (final amount)
        priority_patterns = [
            (r"Total\s+Incl(?:uding)?\s+VAT[:\s\|]*[A-Z]{3}\s+([\d,]+\.?\d*)", 1.0),  # "Total Incl VAT | AED 4,473.48"
            (r"Total\s+Incl(?:uding)?\s+VAT[:\s\|]*([\d,]+\.?\d*)", 0.99),  # Without currency code
            (r"Invoice\s+Total\s+[A-Z]{3}[:\s]*([\d,]+\.?\d*)", 0.95),
            (r"Amount\s+Due\s+[A-Z]{3}[:\s]*([\d,]+\.?\d*)", 0.94),
            (r"Grand\s+Total[:\s]*\$?([\d,]+\.?\d*)", 0.9),
            (r"Total\s+[A-Z]{3}[:\s]*([\d,]+\.?\d*)", 0.85),
            (r"Final\s+Amount[:\s]*\$?([\d,]+\.?\d*)", 0.8),
        ]
        
        amounts_with_priority = []
        for pattern, priority in priority_patterns:
            matches = re.findall(pattern, clean_text, re.IGNORECASE)
            for match in matches:
                try:
                    amount_str = match.replace(',', '')
                    amount = float(amount_str)
                    if amount > 0:
                        amounts_with_priority.append((amount, priority))
                except ValueError:
                    continue
        
        # If we found priority amounts, return the highest priority one
        if amounts_with_priority:
            amounts_with_priority.sort(key=lambda x: x[1], reverse=True)
            return amounts_with_priority[0][0]
        
        # Fallback to all patterns
        patterns = [
            r"Total\s+Incl(?:uding)?\s+VAT[:\s\|]*[A-Z]{3}\s+([\d,]+\.?\d*)",  # "Total Incl VAT | AED 4,473.48"
            r"Total\s+Incl(?:uding)?\s+VAT[:\s\|]*([\d,]+\.?\d*)",  # Total including VAT without currency
            r"Invoice\s+Total[:\s]*\$?([\d,]+\.?\d*)",
            r"Amount\s+Due[:\s]*\$?([\d,]+\.?\d*)",
            r"Grand\s+Total[:\s]*\$?([\d,]+\.?\d*)",
            r"Total\s+Amount[:\s]*\$?([\d,]+\.?\d*)",
            r"Final\s+Amount[:\s]*\$?([\d,]+\.?\d*)",
            r"Net\s+Amount[:\s]*\$?([\d,]+\.?\d*)",
            r"Total[:\s]*\$?([\d,]+\.?\d*)",
            r"Amount[:\s]*\$?([\d,]+\.?\d*)",
            r"Balance[:\s]*\$?([\d,]+\.?\d*)",
            r"Due[:\s]*\$?([\d,]+\.?\d*)",
        ]
        
        amounts = []
        for pattern in patterns:
            matches = re.findall(pattern, clean_text, re.IGNORECASE)
            for match in matches:
                try:
                    amount_str = match.replace(',', '')
                    amount = float(amount_str)
                    if amount > 0:
                        amounts.append(amount)
                except ValueError:
                    continue
        
        # Return the highest amount found (likely the total)
        if amounts:
            return max(amounts)
        
        return 0.0
    
    def _extract_currency(self, text: str) -> str:
        """Extract currency with improved detection - look for currency codes near amounts"""
        clean_text = text.strip()
        
        # Look for currency codes near invoice total or amount due
        currency_patterns = [
            (r'Invoice\s+Total\s+([A-Z]{3})', 1.0),  # Highest priority
            (r'Amount\s+Due\s+([A-Z]{3})', 0.95),
            (r'Total\s+([A-Z]{3})', 0.9),
            (r'Currency\s+([A-Z]{3})', 0.85),
        ]
        
        for pattern, priority in currency_patterns:
            match = re.search(pattern, clean_text, re.IGNORECASE)
            if match:
                currency = match.group(1).upper()
                if currency in ['USD', 'EUR', 'GBP', 'JPY', 'INR', 'AED', 'CAD', 'AUD', 'CHF', 'CNY']:
                    return currency
        
        # Currency symbol patterns
        currency_symbols = [
            (r'\$', 'USD'),
            (r'€', 'EUR'),
            (r'£', 'GBP'),
            (r'¥', 'JPY'),
            (r'₹', 'INR'),
            (r'₽', 'RUB'),
            (r'₩', 'KRW'),
            (r'₪', 'ILS'),
            (r'₦', 'NGN'),
            (r'₨', 'PKR')
        ]
        
        for pattern, currency in currency_symbols:
            if re.search(pattern, clean_text):
                return currency
        
        # Currency code patterns
        currency_codes = [
            r'\bAED\b', r'\bUSD\b', r'\bEUR\b', r'\bGBP\b', r'\bJPY\b', r'\bINR\b',
            r'\bRUB\b', r'\bKRW\b', r'\bILS\b', r'\bNGN\b', r'\bPKR\b',
            r'\bCAD\b', r'\bAUD\b', r'\bCHF\b', r'\bCNY\b', r'\bSEK\b',
            r'\bNOK\b', r'\bDKK\b', r'\bPLN\b', r'\bCZK\b', r'\bHUF\b'
        ]
        
        for code in currency_codes:
            if re.search(code, clean_text, re.IGNORECASE):
                return code.replace('\\b', '').upper()
        
        # Default to USD if no currency found
        return 'USD'
    
    def _extract_date(self, text: str) -> str:
        """Extract document date with improved patterns - handles multiple date formats"""
        clean_text = text.strip()
        lines = clean_text.split('\n')
        
        # Look for "Invoice Date" or "Date" label and get the value on the next line
        for i, line in enumerate(lines):
            line_lower = line.lower().strip()
            if 'invoice date' in line_lower or ('date' in line_lower and 'invoice' in clean_text[max(0, i-2):i+2].lower()):
                # Get the next non-empty line which should be the date
                for j in range(i + 1, min(i + 3, len(lines))):
                    next_line = lines[j].strip()
                    if next_line and len(next_line) > 5:
                        # Try to parse date in various formats
                        date_str = self._parse_date_string(next_line)
                        if date_str:
                            return date_str
        
        # Enhanced date patterns - handle multiple formats
        patterns = [
            # Format: "13 Sep 2025" or "13 September 2025"
            (r"Invoice\s+Date[:\s]*\n\s*(\d{1,2}\s+[A-Za-z]+\s+\d{4})", True),
            (r"Date[:\s]*\n\s*(\d{1,2}\s+[A-Za-z]+\s+\d{4})", True),
            # Format: "13/09/2025" or "13-09-2025"
            (r"Invoice\s+Date[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", False),
            (r"Date[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", False),
            (r"Issue\s+Date[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", False),
            (r"Created[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", False),
        ]
        
        for pattern, needs_parsing in patterns:
            match = re.search(pattern, clean_text, re.IGNORECASE | re.MULTILINE)
            if match:
                date_str = match.group(1)
                if needs_parsing:
                    parsed = self._parse_date_string(date_str)
                    if parsed:
                        return parsed
                else:
                    try:
                        if '/' in date_str:
                            parts = date_str.split('/')
                            if len(parts) == 3:
                                if len(parts[2]) == 2:
                                    parts[2] = '20' + parts[2]
                                return f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
                        elif '-' in date_str:
                            return date_str
                    except:
                        continue
        
        return datetime.now().strftime("%Y-%m-%d")
    
    def _parse_date_string(self, date_str: str) -> Optional[str]:
        """Parse date string in various formats like '13 Sep 2025'"""
        # Manual parsing for common formats
        month_map = {
            'jan': '01', 'january': '01',
            'feb': '02', 'february': '02',
            'mar': '03', 'march': '03',
            'apr': '04', 'april': '04',
            'may': '05',
            'jun': '06', 'june': '06',
            'jul': '07', 'july': '07',
            'aug': '08', 'august': '08',
            'sep': '09', 'september': '09',
            'oct': '10', 'october': '10',
            'nov': '11', 'november': '11',
            'dec': '12', 'december': '12'
        }
        # Pattern: "13 Sep 2025" or "13 September 2025"
        match = re.search(r'(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})', date_str, re.IGNORECASE)
        if match:
            day, month, year = match.groups()
            month_lower = month.lower()
            month_num = month_map.get(month_lower) or month_map.get(month_lower[:3])
            if month_num:
                return f"{year}-{month_num}-{day.zfill(2)}"
        return None
    
    def _extract_due_date(self, text: str) -> Optional[str]:
        """Extract due date with improved patterns - handles multiple date formats
        Works for all document types: PO, Invoice, Contract, Service Agreement
        Looks for: Due Date, Payment Due, Expiry Date, Valid Until, End Date, Expiration Date
        """
        clean_text = text.strip()
        lines = clean_text.split('\n')
        
        # Look for various date labels (due date, expiry date, valid until, etc.)
        date_labels = [
            'due date', 'payment due', 'expiry date', 'expiration date',
            'valid until', 'valid till', 'end date', 'expires on',
            'contract end', 'agreement end', 'service end', 'term end'
        ]
        
        for i, line in enumerate(lines):
            line_lower = line.lower().strip()
            if any(label in line_lower for label in date_labels):
                # Get the next non-empty line which should be the date
                for j in range(i + 1, min(i + 3, len(lines))):
                    next_line = lines[j].strip()
                    if next_line and len(next_line) > 5:
                        # Try to parse date in various formats
                        date_str = self._parse_date_string(next_line)
                        if date_str:
                            return date_str
                        # Try standard date formats
                        date_match = re.search(r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', next_line)
                        if date_match:
                            date_str = date_match.group(1)
                            try:
                                if '/' in date_str:
                                    parts = date_str.split('/')
                                    if len(parts) == 3:
                                        if len(parts[2]) == 2:
                                            parts[2] = '20' + parts[2]
                                        return f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
                                elif '-' in date_str:
                                    return date_str
                            except:
                                continue
        
        # Enhanced due date patterns - handle multiple formats for all document types
        patterns = [
            # Format: "13 Nov 2025" or "13 November 2025"
            (r"Due\s+Date[:\s]*\n\s*(\d{1,2}\s+[A-Za-z]+\s+\d{4})", True),
            (r"Due\s+Date[:\s]*(\d{1,2}\s+[A-Za-z]+\s+\d{4})", True),
            (r"Payment\s+Due[:\s]*(\d{1,2}\s+[A-Za-z]+\s+\d{4})", True),
            (r"Expiry\s+Date[:\s]*(\d{1,2}\s+[A-Za-z]+\s+\d{4})", True),
            (r"Expiration\s+Date[:\s]*(\d{1,2}\s+[A-Za-z]+\s+\d{4})", True),
            (r"Valid\s+Until[:\s]*(\d{1,2}\s+[A-Za-z]+\s+\d{4})", True),
            (r"End\s+Date[:\s]*(\d{1,2}\s+[A-Za-z]+\s+\d{4})", True),
            # Format: "13/11/2025" or "13-11-2025"
            (r"Due\s+Date[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", False),
            (r"Payment\s+Due[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", False),
            (r"Expiry\s+Date[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", False),
            (r"Expiration\s+Date[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", False),
            (r"Valid\s+Until[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", False),
            (r"End\s+Date[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", False),
        ]
        
        for pattern, needs_parsing in patterns:
            match = re.search(pattern, clean_text, re.IGNORECASE | re.MULTILINE)
            if match:
                date_str = match.group(1)
                if needs_parsing:
                    parsed = self._parse_date_string(date_str)
                    if parsed:
                        return parsed
                else:
                    try:
                        if '/' in date_str:
                            parts = date_str.split('/')
                            if len(parts) == 3:
                                if len(parts[2]) == 2:
                                    parts[2] = '20' + parts[2]
                                return f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
                        elif '-' in date_str:
                            return date_str
                    except:
                        continue
        
        return None
    
    def _extract_po_number(self, text: str) -> Optional[str]:
        """Extract PO number with improved patterns - exclude Reference field"""
        clean_text = text.strip()
        lines = clean_text.split('\n')
        
        # Look for PO-related labels and get the value
        for i, line in enumerate(lines):
            line_lower = line.lower().strip()
            # Skip "Reference" as it's not a PO number
            if 'po number' in line_lower or 'purchase order' in line_lower or ('po' in line_lower and '#' in line_lower):
                # Get the next non-empty line
                for j in range(i + 1, min(i + 3, len(lines))):
                    next_line = lines[j].strip()
                    if next_line and len(next_line) > 2:
                        # Extract alphanumeric PO number
                        po_match = re.search(r'([A-Z0-9/_-]+)', next_line)
                        if po_match:
                            po_number = po_match.group(1).strip()
                            if len(po_number) > 2:
                                return po_number
        
        # Enhanced PO number patterns (fallback) - exclude Reference
        patterns = [
            r"PO\s+Number[:\s]*\n\s*([A-Z0-9/_-]+)",
            r"Purchase\s+Order\s+Number[:\s]*\n\s*([A-Z0-9/_-]+)",
            r"PO\s*#?\s*([A-Z0-9/_-]+)",
            r"Purchase\s+Order\s*#?\s*([A-Z0-9/_-]+)",
            r"P\.O\.\s*#?\s*([A-Z0-9/_-]+)",
            r"PO\s+Number[:\s]*([A-Z0-9/_-]+)",
            r"Order\s+Number[:\s]*([A-Z0-9/_-]+)",
        ]
        
        for pattern in patterns:
            match = re.search(pattern, clean_text, re.IGNORECASE | re.MULTILINE)
            if match:
                po_number = match.group(1).strip()
                # Skip if it looks like a person's name (common in Reference field)
                if len(po_number) > 2 and not po_number.lower().islower():
                    return po_number
        
        return None
    
    def _extract_invoice_number(self, text: str) -> Optional[str]:
        """Extract invoice number with improved patterns"""
        clean_text = text.strip()
        lines = clean_text.split('\n')
        
        # Look for "Invoice Number" label and get the value on the next line
        for i, line in enumerate(lines):
            line_lower = line.lower().strip()
            if 'invoice number' in line_lower or 'invoice #' in line_lower:
                # Get the next non-empty line which should be the invoice number
                for j in range(i + 1, min(i + 3, len(lines))):
                    next_line = lines[j].strip()
                    if next_line and len(next_line) > 2:
                        # Extract alphanumeric invoice number (may contain /, -, etc.)
                        invoice_match = re.search(r'([A-Z0-9/_-]+)', next_line)
                        if invoice_match:
                            invoice_number = invoice_match.group(1).strip()
                            if len(invoice_number) > 2:
                                return invoice_number
        
        # Enhanced invoice number patterns (fallback)
        patterns = [
            r"Invoice\s+Number[:\s]*\n\s*([A-Z0-9/_-]+)",
            r"Invoice\s*#?\s*([A-Z0-9/_-]+)",
            r"Inv\s*#?\s*([A-Z0-9/_-]+)",
            r"Bill\s*#?\s*([A-Z0-9/_-]+)",
            r"Invoice\s+Number[:\s]*([A-Z0-9/_-]+)",
            r"Bill\s+Number[:\s]*([A-Z0-9/_-]+)",
        ]
        
        for pattern in patterns:
            match = re.search(pattern, clean_text, re.IGNORECASE | re.MULTILINE)
            if match:
                invoice_number = match.group(1).strip()
                if len(invoice_number) > 2:
                    return invoice_number
        
        return None
    
    def _extract_vendor_address(self, text: str) -> Optional[str]:
        """Extract vendor address with improved patterns"""
        clean_text = text.strip()
        lines = clean_text.split('\n')
        
        # Enhanced vendor address patterns
        vendor_keywords = ['vendor', 'supplier', 'from', 'company address', 'business address', 'headquarters']
        
        for i, line in enumerate(lines):
            line_lower = line.lower()
            if any(keyword in line_lower for keyword in vendor_keywords):
                address_lines = []
                for j in range(i+1, min(i+6, len(lines))):
                    current_line = lines[j].strip()
                    if current_line and len(current_line) > 5:
                        if (re.search(r'\d+', current_line) or
                            re.search(r'(street|st|avenue|ave|road|rd|boulevard|blvd|drive|dr|lane|ln)', current_line, re.IGNORECASE) or
                            re.search(r'(city|state|zip|postal|postal code)', current_line, re.IGNORECASE)):
                            address_lines.append(current_line)
                        elif len(address_lines) > 0:
                            break
                
                if address_lines:
                    return ' '.join(address_lines)
        
        return None
    
    def _extract_client_address(self, text: str) -> Optional[str]:
        """Extract client address with improved patterns"""
        clean_text = text.strip()
        lines = clean_text.split('\n')
        
        # Enhanced client address patterns
        client_keywords = ['client', 'customer', 'bill to', 'billed to', 'ship to', 'shipping address', 'billing address']
        
        for i, line in enumerate(lines):
            line_lower = line.lower()
            if any(keyword in line_lower for keyword in client_keywords):
                address_lines = []
                for j in range(i+1, min(i+6, len(lines))):
                    current_line = lines[j].strip()
                    if current_line and len(current_line) > 5:
                        if (re.search(r'\d+', current_line) or
                            re.search(r'(street|st|avenue|ave|road|rd|boulevard|blvd|drive|dr|lane|ln)', current_line, re.IGNORECASE) or
                            re.search(r'(city|state|zip|postal|postal code)', current_line, re.IGNORECASE)):
                            address_lines.append(current_line)
                        elif len(address_lines) > 0:
                            break
                
                if address_lines:
                    return ' '.join(address_lines)
        
        return None
    
    def _calculate_confidence(self, text: str, document_type: str) -> float:
        """Calculate confidence score for the extraction"""
        confidence = 0.3  # Base confidence
        
        # Increase confidence based on text length and quality
        text_length = len(text)
        if text_length > 100:
            confidence += 0.1
        if text_length > 500:
            confidence += 0.1
        if text_length > 1000:
            confidence += 0.1
        
        # Increase confidence based on document structure indicators
        structure_indicators = [
            r'\$[\d,]+\.?\d*',
            r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',
            r'[A-Z]{2,}\s*#?\s*[A-Z0-9-]+',
            r'[A-Za-z\s]+,\s*[A-Za-z\s]+,\s*[A-Z]{2,3}',
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        ]
        
        structure_matches = 0
        for pattern in structure_indicators:
            if re.search(pattern, text):
                structure_matches += 1
        
        confidence += (structure_matches / len(structure_indicators)) * 0.2
        
        # Increase confidence based on document type keywords
        type_keywords = {
            "Client PO": ["purchase order", "client", "po", "order", "requisition"],
            "Vendor PO": ["purchase order", "vendor", "supplier", "order", "requisition"],
            "Client Invoice": ["invoice", "client", "bill", "statement", "receipt"],
            "Vendor Invoice": ["invoice", "vendor", "supplier", "bill", "statement"],
            "Service Agreement": ["agreement", "contract", "service", "terms", "conditions"]
        }
        
        if document_type in type_keywords:
            keywords = type_keywords[document_type]
            matches = sum(1 for keyword in keywords if keyword.lower() in text.lower())
            confidence += (matches / len(keywords)) * 0.2
        
        # Increase confidence based on extracted data quality
        extracted_data_quality = 0
        if re.search(r'\$[\d,]+\.?\d*', text):
            extracted_data_quality += 0.1
        if re.search(r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}', text):
            extracted_data_quality += 0.1
        if re.search(r'[A-Z]{2,}\s*#?\s*[A-Z0-9-]+', text):
            extracted_data_quality += 0.1
        if re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text):
            extracted_data_quality += 0.1
        
        confidence += extracted_data_quality
        
        return min(confidence, 0.95)
    
    def _generate_document_id(self, document_type: str) -> str:
        """Generate a unique document ID"""
        prefix_map = {
            "Client PO": "PO-CLIENT",
            "Vendor PO": "PO-VENDOR", 
            "Client Invoice": "INV-CLIENT",
            "Vendor Invoice": "INV-VENDOR",
            "Service Agreement": "AGR"
        }
        
        prefix = prefix_map.get(document_type, "DOC")
        timestamp = datetime.now().strftime("%Y%m%d")
        unique_id = str(uuid.uuid4())[:8].upper()
        
        return f"{prefix}-{timestamp}-{unique_id}"
    
    def _extract_summary(self, text: str) -> str:
        """Extract a summary of the document"""
        sentences = text.split('.')[:3]
        summary = '. '.join(sentences).strip()
        if len(summary) > 200:
            summary = summary[:200] + "..."
        return summary
    
    def _extract_key_terms(self, text: str) -> List[str]:
        """Extract key terms and phrases from the document"""
        key_terms = []
        
        # Financial terms
        financial_patterns = [
            r"Total[:\s]*([A-Z]{3}\s*[\d,]+\.?\d*)",
            r"Amount[:\s]*([A-Z]{3}\s*[\d,]+\.?\d*)",
            r"Due[:\s]*([A-Z]{3}\s*[\d,]+\.?\d*)",
            r"Payment[:\s]*([A-Z]{3}\s*[\d,]+\.?\d*)"
        ]
        
        for pattern in financial_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            key_terms.extend(matches)
        
        # Document numbers
        doc_patterns = [
            r"PO[:\s]*([A-Z0-9-]+)",
            r"Invoice[:\s]*([A-Z0-9-]+)",
            r"Reference[:\s]*([A-Z0-9-]+)"
        ]
        
        for pattern in doc_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            key_terms.extend(matches)
        
        return list(set(key_terms))[:10]
    
    def _extract_contact_info(self, text: str) -> Dict[str, str]:
        """Extract contact information"""
        contact_info = {}
        
        # Email addresses
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails = re.findall(email_pattern, text)
        if emails:
            contact_info['emails'] = emails[:3]
        
        # Phone numbers - more precise pattern
        # Match exactly 10 digits (standard) or 11 digits (with extension)
        # Allow optional + at start, and formatting characters (spaces, dashes, parentheses)
        # Exclude patterns that are clearly reference numbers, TRN, or other IDs
        lines = text.split('\n')
        phones = []
        excluded_labels = ['reference', 'ref', 'trn', 'tax id', 'tax identification', 
                          'vat', 'registration', 'reg', 'id', 'invoice number', 
                          'po number', 'purchase order', 'account number', 'account no',
                          'document number', 'doc number', 'file number']
        
        # Pattern for phone numbers: +? followed by 10-11 digits with optional formatting
        # Examples: +1-234-567-8900, (123) 456-7890, 123-456-7890, +971501234567
        phone_patterns = [
            # International format with +: +1-234-567-8900, +971-50-123-4567
            r'\+\d{1,3}[\s\-]?\d{1,4}[\s\-]?\d{1,4}[\s\-]?\d{4,6}',
            # Standard format with parentheses: (123) 456-7890
            r'\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{4}',
            # Simple format: 123-456-7890, 123.456.7890, 123 456 7890
            r'\d{3}[\s\-\.]?\d{3}[\s\-\.]?\d{4}',
            # 11 digits with extension: 123-456-7890-1
            r'\d{3}[\s\-]?\d{3}[\s\-]?\d{4}[\s\-]?\d{1}',
        ]
        
        for i, line in enumerate(lines):
            line_lower = line.lower().strip()
            
            # Skip lines that contain excluded labels (check current and previous line)
            context = ' '.join([lines[max(0, i-1)].lower(), line_lower, 
                               lines[min(len(lines)-1, i+1)].lower()])
            if any(label in context for label in excluded_labels):
                continue
            
            # Check if line contains phone-related keywords (optional, but helps)
            phone_keywords = ['phone', 'tel', 'mobile', 'cell', 'contact', 'fax']
            has_phone_keyword = any(keyword in line_lower for keyword in phone_keywords)
            
            for pattern in phone_patterns:
                matches = re.findall(pattern, line)
                for match in matches:
                    # Count actual digits in the match
                    digit_count = len(re.sub(r'\D', '', match))
                    
                    # Must be exactly 10 or 11 digits
                    if digit_count != 10 and digit_count != 11:
                        continue
                    
                    # Additional validation: exclude if it looks like a reference/ID number
                    # Reference numbers often have specific patterns (all caps, specific lengths, etc.)
                    # If the match is in a line with excluded labels nearby, skip it
                    match_upper = match.upper()
                    if re.match(r'^[A-Z0-9\-]+$', match_upper) and not has_phone_keyword:
                        # If it's all uppercase alphanumeric and no phone keyword, likely not a phone
                        if len(match.replace('-', '').replace(' ', '')) > 10:
                            continue
                    
                    # Clean up the phone number
                    cleaned = re.sub(r'[\s\-\(\)\.]', '', match)
                    # If it starts with +, keep it
                    if match.startswith('+'):
                        cleaned = '+' + cleaned.lstrip('+')
                    
                    # Additional validation: if no phone keyword, be more strict
                    # Must have formatting (spaces, dashes, parentheses) or start with +
                    if not has_phone_keyword:
                        if not (re.search(r'[\s\-\(\)]', match) or match.startswith('+')):
                            # If it's just digits without formatting and no phone keyword, skip
                            continue
                    
                    # Final check: don't add if it's already in the list
                    if cleaned not in phones:
                        phones.append(cleaned)
                        if len(phones) >= 3:
                            break
                if len(phones) >= 3:
                    break
            if len(phones) >= 3:
                break
        
        if phones:
            contact_info['phones'] = phones[:3]
        
        # Addresses (simple pattern)
        address_pattern = r'([A-Za-z\s]+,\s*[A-Za-z\s]+,\s*[A-Z]{2,3})'
        addresses = re.findall(address_pattern, text)
        if addresses:
            contact_info['addresses'] = addresses[:2]
        
        return contact_info
