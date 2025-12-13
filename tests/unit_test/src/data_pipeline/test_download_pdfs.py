from unittest.mock import patch

from src.data_pipeline.load.download_pdf import downloadPDF


def test_downloadPDF_skips_existing(tmp_path):
    # Setup a fake PDF path that already exists
    category = "2025R"
    bill_no = "HB1001"
    pdf_dir = tmp_path / category
    pdf_dir.mkdir(parents=True)
    pdf_path = pdf_dir / f"{bill_no}.pdf"
    pdf_path.write_text("existing content")

    with patch("src.data_pipeline.load.download_pdf.PDF_DIR", tmp_path):
        returned_path = downloadPDF(category, bill_no, "http://fakeurl.com/fake.pdf")
        assert returned_path == pdf_path
        # File contents are unchanged
        assert pdf_path.read_text() == "existing content"


def test_downloadPDF_downloads_new(tmp_path):
    category = "2025R"
    bill_no = "HB1002"
    fake_content = b"PDF CONTENT"

    with patch("src.data_pipeline.load.download_pdf.requests.get") as mock_get, \
         patch("src.data_pipeline.load.download_pdf.PDF_DIR", tmp_path):

        mock_get.return_value.status_code = 200
        mock_get.return_value.content = fake_content

        returned_path = downloadPDF(category, bill_no, "http://fakeurl.com/fake.pdf")
        assert returned_path.exists()
        assert returned_path.read_bytes() == fake_content


