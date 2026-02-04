from pathlib import Path

import pdfplumber

class PdfProcessor:



    def process_pdf(self, path: Path):
        with pdfplumber.open("./data/pdf/2003_R/hb/hb1001/hb1001_bill_text_0.pdf") as pdf:
             builder = ""
             sideMargins = 90
             tbMargins = 80
             newlineChecker = 0
             for page in pdf.pages:
                 nPage = page.crop((sideMargins, tbMargins, page.width, page.height - tbMargins))
                 for char in nPage.chars:
                     if char.get('x0') < newlineChecker:
                         print("")
                     newlineChecker = char.get('x0')
                     print(char.get('text'), end="")