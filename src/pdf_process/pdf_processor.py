from pathlib import Path

import pdfplumber

from utils.paths import project_root

sideMargins = 90
tbMargins = 80
line_tolerance = 3
rect_height_max = 5

class PdfProcessor:
    def flush_span(self, buffer, fmt):
        if not buffer:
            return ''
        u, s = fmt
        text = buffer
        if s:
            text = f'~{text}~'
        if u:
            text = f'_{text}_'
        return text

    def process_pdf(self, path: Path):
        with pdfplumber.open(project_root / "data/pdf/2003_R/hb/hb1001/hb1001_bill_text_0.pdf") as pdf:
            final_string = ''
            newlineChecker = 0

            for page in pdf.pages:
                nPage = page.crop((sideMargins, tbMargins, page.width, page.height - tbMargins))
                previous_bottom = None

                span_buffer = ''
                span_format = (False, False)  # (underline, strikethrough

                for char in nPage.chars:
                    x0, x1 = char['x0'], char['x1']
                    top, bottom = char['top'], char['bottom']

                    underline = False
                    strikethrough = False

                    for rect in nPage.rects:
                        height = rect['bottom'] - rect['top']
                        width_overlap = min(x1, rect['x1']) - max(x0, rect['x0'])
                        if width_overlap > 0 and height <= rect_height_max:
                            if abs(rect['top'] - bottom) <= line_tolerance:
                                underline = True

                    if previous_bottom is not None and top > previous_bottom + 2:
                        print(self.flush_span(span_buffer, span_format), end='\n')
                        span_buffer = ''
                        span_format = (False, False)

                    previous_bottom = bottom

                    is_space = char['text'].isspace()
                    if (underline, strikethrough) != span_format and not is_space:
                        print(self.flush_span(span_buffer, span_format), end='')
                        span_buffer = ''
                        span_format = (underline, strikethrough)
                    else:
                        # Update span_format if current char has formatting
                        span_format = (underline or span_format[0], strikethrough or span_format[1])

                    # Add char to buffer
                    span_buffer += char['text']

                # Flush remaining buffer at end of page
                print(self.flush_span(span_buffer, span_format), end='\n')



if __name__ == "__main__":
    PdfProcessor().process_pdf(Path(''))