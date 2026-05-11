"""pdf_cleaner.py 的测试脚本"""
import os
from pathlib import Path

work_dir = Path.home() / "Desktop" / "coding" / "data_analysis" / "financial_pipeline"
os.chdir(work_dir)

from pdf_cleaner import PDFCleaner, DocumentChunker

# 1. 提取文本
cleaner = PDFCleaner()
text = cleaner.extract_text("annual_reports/300126_锐奇股份A_2025.pdf")

# 2. 分块
chunker = DocumentChunker(chunk_size=500, overlap=100)
chunks = chunker.chunk_by_heading(text, source="锐奇股份_2025")
info = chunker.report(chunks)

# 输出示例:
# ========================================
# 分块统计报告
#   total_chunks: 47
#   unique_sources: 1
#   avg_chunk_len: 423.0
#   max_chunk_len: 998
#   min_chunk_len: 28
#   sources: ['锐奇股份_2025']