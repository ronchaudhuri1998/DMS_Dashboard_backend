from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from fastapi.responses import FileResponse
from typing import List
from sqlalchemy.orm import Session
from app.database import get_db
from app.services.upload_service import UploadService
from app.schemas import UploadResponse
import os

router = APIRouter(prefix="/api/uploads", tags=["uploads"])

@router.post("/", response_model=UploadResponse)
async def upload_files(files: List[UploadFile] = File(...)):
    """Upload multiple files"""
    try:
        upload_service = UploadService()
        response = await upload_service.upload_files(files)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload files: {str(e)}")

@router.get("/{filename}")
async def get_file(filename: str):
    """Get an uploaded file"""
    try:
        upload_service = UploadService()
        file_path = upload_service.get_file_path(filename)
        
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="File not found")
        
        return FileResponse(file_path)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve file: {str(e)}")

@router.delete("/{filename}")
async def delete_file(filename: str):
    """Delete an uploaded file"""
    try:
        upload_service = UploadService()
        success = upload_service.delete_file(filename)
        
        if not success:
            raise HTTPException(status_code=404, detail="File not found")
        
        return {"message": "File deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete file: {str(e)}")

@router.post("/process/{filename}")
async def process_pdf(filename: str, db: Session = Depends(get_db)):
    """Process an uploaded PDF file and extract data"""
    try:
        upload_service = UploadService()
        result = await upload_service.process_uploaded_pdf(filename, db)
        
        if not result.get("success", False):
            error_message = result.get("error", "Processing failed")
            # Log the error for debugging
            print(f"❌ Processing failed for {filename}: {error_message}")
            raise HTTPException(
                status_code=400, 
                detail=error_message,
                headers={"X-Error-Type": result.get("error_type", "processing_error")}
            )
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        error_msg = f"Failed to process PDF: {str(e)}"
        print(f"❌ Exception processing {filename}: {error_msg}")
        raise HTTPException(status_code=500, detail=error_msg)
