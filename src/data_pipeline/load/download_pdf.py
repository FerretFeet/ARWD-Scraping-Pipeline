import time
from pathlib import Path

import requests

from src.utils.logger import logger
from src.utils.paths import project_root

PDF_DIR = project_root / "data" / "pdf"


def downloadPDF(category: str, bill_no: str, url: str) -> Path | None:
    """
    Download a PDF from a URL and save it locally.
    
    returns Path(PDF_DIR)/category/bill_no.pdf.
    """
    if not url: return None
    #Request PDF
    category_dir = PDF_DIR / category
    category_dir.mkdir(parents=True, exist_ok=True)

    pdf_path = category_dir / f"{bill_no}.pdf"

    # Skip download if file already exists
    if pdf_path.exists():
        logger.info(f"PDF already exists: {pdf_path}")
        return pdf_path

    response = requests.get(url)
    if response.status_code != 200:  # noqa: PLR2004
        msg = f"Failed to download PDF ({response.status_code})"
        raise Exception(msg)
    #Search if already exists
    pdf_path.write_bytes(response.content)
    time.sleep(0.5) # Courtesy
    logger.info(f"PDF downloaded to: {pdf_path}")

    return pdf_path
