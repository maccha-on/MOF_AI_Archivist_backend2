import os
import io
import fitz  # PyMuPDF
from azure.storage.blob import BlobServiceClient

blob_service = BlobServiceClient.from_connection_string(
    os.getenv("AZURE_STORAGE_CONNECTION_STRING")
)

def load_pdfs_from_blob(container_name: str, prefix: str | None):
    container = blob_service.get_container_client(container_name)
    pages: list[dict] = []

    for blob in container.list_blobs(name_starts_with=prefix):
        if not blob.name.lower().endswith(".pdf"):
            continue

        # PDFをBlobから読み込み
        pdf_bytes = container.download_blob(blob.name).readall()
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")

        file_name = os.path.basename(blob.name)
        blob_url = container.get_blob_client(blob.name).url

        for page_index in range(len(doc)):
            page = doc.load_page(page_index)

            # ★ ここが重要：PyMuPDFの日本語対応抽出
            text = page.get_text("text")

            if not text or not text.strip():
                continue  # 空ページはスキップ

            pages.append({
                "documentName": file_name,
                "blobUrl": blob_url,
                "page": page_index + 1,
                "text": text
            })

        doc.close()

    return pages
