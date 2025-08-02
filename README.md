# 🏦 Fed Data Scraper

**Fed Data Scraper** is a pipeline for extracting and structuring data from Federal Reserve Y-6 filings. It uses OCR, LLMs, and markdown parsing to turn unstructured PDFs into usable CSVs.

---

## 🚀 Features

- Reads documents from AWS S3
- OCR via Mistral
- LLM data parsing using Gemini
- Markdown → Structured JSON → CSV
- Supports insider and shareholder data
- Tracks success/failure status per file

---

## ⚙️ Configuration via `.env`

All sensitive credentials are stored in a `.env` file for secure and flexible usage.

### 📝 Sample `.env` Format

\`\`\`env
COOKIES='[
    {"name": "BrowserGUID", "value": "...", "domain": ".capitaliq.spglobal.com"},
    {"name": "CIQP", "value": "true", "domain": ".capitaliq.spglobal.com"},
    {"name": "EKOU", "value": "...", "domain": ".capitaliq.spglobal.com"},
    ...
    {"name": "SNL_OAUTH_TOKEN1", "value": "...", "domain": ".capitaliq.spglobal.com"}
]'
MISTRAL_API_KEY='your_mistral_api_key_here'
GENAI_API_KEY='your_google_gemini_api_key_here'
\`\`\`

> 💡 You can retrieve \`COOKIES\` by exporting from your browser (e.g., using Chrome DevTools or browser extensions like "EditThisCookie").

---

## 🧠 File Overview

### `scraper.py`
- Downloads documents and extracts markdown via OCR.
- Sends PDF URLs to Mistral API.
- Stores markdown to S3 and `json/`.

### `read_json.py`
- Reads markdown from `json/`.
- Uses Gemini API to extract structured data.
- Outputs CSVs for insiders and shareholders.

### `read_pdfs.py`
- Alternative flow: direct PDF parsing using `pdfplumber`.
- Bypasses OCR for debugging or fallback.

---

## 🗂 Folder Structure

\`\`\`
fed-data-scraper/
├── scraper.py
├── read_json.py
├── read_pdfs.py
├── .env               # Your credentials live here
├── json/              # OCR markdown files
├── csv/               # Parsed insider/shareholder CSVs
└── README.md
\`\`\`

---

## ⬇️ Setup Instructions

### 1. Clone the Repository

\`\`\`bash
git clone https://github.com/yourusername/fed-data-scraper.git
cd fed-data-scraper
\`\`\`

### 2. Install Dependencies

\`\`\`bash
pip install -r requirements.txt
\`\`\`

> Required packages include: `boto3`, `pdfplumber`, `pandas`, `google-generativeai`, `mistralai`, `python-dotenv`, etc.

### 3. Configure `.env`

Add your credentials to a file called `.env` in the root directory, using the format above.

---

## 🧪 Usage

### OCR Pipeline: PDF to markdown

\`\`\`bash
python scraper.py
\`\`\`

### LLM Parsing: markdown to CSV

\`\`\`bash
python read_json.py
\`\`\`

### (Optional) Local PDF Parsing

\`\`\`bash
python read_pdfs.py
\`\`\`

---

## 📁 Output

- CSVs saved locally to `/csv/` and uploaded to S3 (if configured).
- Logs for failed and successful file parses.

---

## 🛑 Notes

- Ensure your S3 bucket follows the expected structure: `/documents/`, `/json/`, and `/csv/`.
- OCR and LLM performance varies based on PDF quality.
- Requires valid Mistral and Google API keys.