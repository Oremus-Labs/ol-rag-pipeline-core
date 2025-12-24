from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RenderedPdfPage:
    page_number: int  # 1-based
    png_bytes: bytes
    width_px: int
    height_px: int


def render_pdf_to_png_pages(
    pdf_bytes: bytes,
    *,
    dpi: int = 200,
    max_pages: int | None = None,
) -> list[RenderedPdfPage]:
    """
    Render a PDF to per-page PNG bytes.

    Uses PyMuPDF (fitz) which is robust and does not require poppler.
    """
    if dpi <= 0:
        raise ValueError("dpi must be > 0")

    import fitz  # type: ignore[import-not-found]

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        pages: list[RenderedPdfPage] = []
        zoom = dpi / 72.0
        matrix = fitz.Matrix(zoom, zoom)
        for idx in range(doc.page_count):
            if max_pages is not None and len(pages) >= max_pages:
                break
            page = doc.load_page(idx)
            pix = page.get_pixmap(matrix=matrix, alpha=False)
            pages.append(
                RenderedPdfPage(
                    page_number=idx + 1,
                    png_bytes=pix.tobytes("png"),
                    width_px=pix.width,
                    height_px=pix.height,
                )
            )
        return pages
    finally:
        doc.close()

