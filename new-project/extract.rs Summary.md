---
title: extract.rs Summary
type: note
permalink: extract.rs-summary
---

**Purpose**: Extracts structured text from EPUB and PDF files in Calibre library with metadata
**Key functionality**:
- Parses EPUB metadata via OPF files (XML processing)
- Classifies PDF quality (text, low-quality, scan) to choose extraction method
- Splits large PDFs into chunks using docling/pdftotext
- Generates JSON metadata for extracted documents
**Critical flow**: Scan library → Detect file type → Extract content → Generate metadata → Save output