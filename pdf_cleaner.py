"""
PDFCleaner - PDF 年报文本&表格提取
"""

import re#表示导入正则表达式来辅助清理pdf
import os
from pathlib import Path
import pdfplumber
import json
import hashlib
from typing import List,Dict

work_dir = Path.home()/"Desktop"/"coding"/"data_analysis"/"financial_pipeline"
os.chdir(work_dir)
from utils import setup_logger

logger = setup_logger(__name__)

class PDFCleaner:
    """PDF 年报解析：提取纯文本+表格"""

    def extract_text(self,pdf_path:str) -> str:
        """
        从单个pdf提取纯文本

        参数:
            pdf_path: the pdf_path
        output:
            the whole text with pages and kill the headers and the footer
        
        """
        path = Path(pdf_path)
        pages = []

        with pdfplumber.open(path) as pdf:
            for i,page in enumerate(pdf.pages,1):
                text = page.extract_text()
                if text:
                    text = self._strip_headers_footers(text)
                    pages.append(f"\n--the {i} page--\n{text}")
        
        result = "\n".join(pages)
        logger.info(f"{path.name}:{len(pdf.pages)} pages, {len(result)} strings")
        return result
    
    def extract_tables(self,pdf_path:str) -> dict:
        """
        get all the tables from the .pdf

        params:
            pdf_path:
        returns:
            {"page_1_table_1":[[...],...]}
        """

        path = Path(pdf_path)
        tables = {}

        with pdfplumber.open(path) as pdf:
            for i,page in enumerate(pdf.pages,1):
                for j,table in enumerate(page.extract_tables(),1):
                    key = f"page_{i}_table_{j}"
                    tables[key] = table
        
        logger.info(f"{path.name}:{len(tables)} tables ")
        return tables
    
    def _strip_headers_footers(self,text:str) -> str:
        """
        kill the common contents that unnacessary:
            - the title line (like:"2025 annual report")
            - page(the page number on the bottom)
            - NaN str or empty
        """

        text = re.sub(r"\d{4}\s*年年度报告\s*","",text)
        text = re.sub(r"\x00","",text)
        text = re.sub(r" {2,}"," ",text)
        text = re.sub(r"\n{3,}","\n\n",text)

        return text.strip()
    
    def clean_text(self,text:str) -> str:
        """
        deeply clean the text you have got

        顺序:空字符 -> 全角转半角 -> 合并空行
        """
        text = text.replace("\x00","")

        text = self._full_to_half(text)

        text = re.sub(r"\n{3,}","\n\n",text)

        return text.strip()

    def _full_to_half(self,text:str) -> str:
        """
    全角字符转半角

    影响范围:
        全角空格　 → 半角空格
        全角标点！～ → 半角 !~
        全角数字０-９ → 0-9
        全角字母Ａ-Ｚ → A-Z

    不受影响:
        中文汉字（不在 0xFF01-0xFF5E 范围内）
    """
        result = []
        for ch in text:
            code = ord(ch)
            if code == 0x3000:
                code = 0x0020
            elif 0xFF01 <= code <= 0xFF5E:
                code -= 0xFEE0
            result.append(chr(code))
        return "".join(result)
    
    def table_to_markdown(self,table:list) -> str:
        """
    将二维列表转为 Markdown 表格

    输入: [["列A", "列B"], ["值1", "值2"]]
    输出:
        | 列A | 列B |
        |:---|---:|
        | 值1 | 值2 |
    """
        if not table or len(table) < 1:
            return ""
        
        header = [str(c).strip() if c else"" for c in table[0]]
        md = "| "+" | ".join(header)+" |\n"
        md += "|"+"|".join(["---" for _ in header])+"|\n"

        for row in table[1:]:
            cells = [
                str(c).strip().replace("\n"," ").replace("|","\\|") if c else ""
                for c in row
            ]
            md += "| "+" | ".join(cells)+" |\n"
        return md
    
    def process_single_pdf(self,pdf_path:str) -> dict:
        """
        端到端处理单个 PDF：提取 → 清洗 → 转 Markdown → 保存

        返回: {"text": ..., "tables": ..., "output": "xxx.md"}
        """
        path = Path(pdf_path)
        text = self.extract_text(str(path))
        text = self.clean_text(text)

        raw_tables = self.extract_tables(str(path))
        md_tables = {}
        for key,tbl in raw_tables.items():
            if tbl:
                md_tables[key] = self.table_to_markdown(tbl)

        md_path = path.with_suffix(".md")
        with open(md_path,"w",encoding = "utf-8") as f:
            f.write(f"# {path.stem}\n\n")
            f.write(text)
            if md_tables:
                f.write("\n\n ## tables\n\n")
                for key,md_table in md_tables.items():
                    f.write(f"###{key}\n")
                    f.write(md_table)
                    f.write("\n\n")
        logger.info(f"Output:{md_path}")
        return {"text":text,"tables":md_tables,"output":str(md_path)}


class DocumentChunker:
    """
    文档分块器 — 为 RAG / 向量检索预处理金融文档

    三种策略:
      chunk_by_heading — 按标题层级切分（适合年报/法律文书）
      chunk_by_paragraph — 按段落切分
      chunk_fixed — 固定长度 + 重叠
    """
    def __init__(self,chunk_size:int = 500,overlap:int = 100):
        self.chunk_size = chunk_size
        self.overlap = overlap
        self._counter = 0

    def chunk_by_heading(self,text:str,source:str) -> List[Dict]:
        """
        按中文标题层级切分

        识别模式:
            第X章、第X节、第X条
            (一)、1.、一、、① 等

        首段（第一个标题之前）标记为「前言」。
        """
        pattern = re.compile(
             r'^'
            r'(?:'
            r'第[一二三四五六七八九十百千万\d]+[章节条]|'
            r'[（(][一二三四五六七八九十\d]+[）)]|'
            r'[一二三四五六七八九十]、|'
            r'\d+\.\s|'
            r'[①②③④⑤⑥⑦⑧⑨⑩]'
            r')',
            re.MULTILINE
        )

        matches = list(pattern.finditer(text))
        
        if not matches:
            return self.chunk_fixed(text,source)
        
        chunks = []

        if matches[0].start() > 0:
            head = text[:matches[0].start()].strip()
            if head:
                chunks.append(self._make_chunk(head,source,heading="前言"))

        for i,m in enumerate(matches):
            heading = m.group().strip()
            body_start = m.end()
            body_end = matches[i+1].start() if i+1 < len(matches) else len(text)
            body = text[body_start:body_end].strip()

            if body:
                if len(body) > self.chunk_size*2:
                    sub = self._split_long_section(body,source,heading)
                    chunks.extend(sub)

                else:
                    chunks.append(self._make_chunk(body,source,heading = heading))
        return chunks
    

    def chunk_by_paragraph(self,text:str,source:str) -> list[Dict]:

        paragraphs = re.split(r"\n{2,}",text)
        chunks = []
        buffer = ""
        buf_heading = ""

        for para in paragraphs:
            para = para.strip()
            if not para:
                continue
        
            if len(buffer) + len(para) <= self.chunk_size:
                buffer += "\n"+para if buffer else para
            else:
                if buffer:
                    chunks.append(self._make_chunk(buffer,source,heading=buf_heading))
                if len(para) > self.chunk_size:
                    sub = self.chunk_fixed(para,source)
                    chunks.extend(sub)
                    buffer = ""
                else:
                    buffer = para
        
        if buffer:
            chunks.append(self._make_chunk(buffer,source,heading = buf_heading))
        return chunks
    

    def chunk_fixed(self,text:str,source:str) -> List[Dict]:

        chunks = []
        start = 0

        while start < len(text):
            end = start + self.chunk_size
            chunk_text = text[start:end].strip()
            if chunk_text:
                chunks.append(self._make_chunk(chunk_text,source))
            start += self.chunk_size - self.overlap
        return chunks
    

    def save_chunks(self,chunks:List[Dict],output_path:str):
        """保存为 JSONL（每行一个 JSON，方便大文件流式读取）"""
        Path(output_path).parent.mkdir(parents = True,exist_ok = True)
        with open(output_path,"w",encoding = "utf-8") as f:
            for chunk in chunks:
                f.write(json.dumps(chunk,ensure_ascii = False) + "\n")
        logger.info(f"saved{len(chunks)} chunks -> {output_path}")

    def _make_chunk(self,text:str,source:str,heading:str = "") -> Dict:
        self._counter += 1
        name = Path(source).stem
        return{
            "text": text,
            "source": source,
            "heading": heading,
            "chunk_id": f"{name}_{self._counter:05d}",
            "char_count": len(text),
        }
    
    def _split_long_section(self,text:str,source:str,heading:str) -> List[Dict]:

        paragraphs = re.split(r"\n{2,}",text)
        chunks = []
        buffer = ""

        for para in paragraphs:
            para = para.strip()
            if not para:
                continue
            if len(buffer) + len(para) <= self.chunk_size:
                buffer += "\n" + para if buffer else para
            else:
                if buffer:
                    chunks.append(self._make_chunk(buffer,source,heading = heading))
                buffer = para
        if buffer:
            chunks.append(self._make_chunk(buffer,source,heading = heading))
        return chunks if chunks else self.chunk_fixed(text,source)
    
    def report(self,chunks:List[Dict]) -> dict:
        """
        生成分块统计报告

        返回: {"total": ..., "avg_len": ..., "sources": [...]}
        """
        if not chunks:
            logger.warning("chunk is empty,fail to generate thr report")
            return {}
        
        sources = list({c["source"] for c in chunks})
        lengths = [c["char_count"] for c in chunks]

        report = {
            "total_chunks":len(chunks),
            "unique_sources":len(sources),
            "avg_chunk_len":round(sum(lengths)/len(lengths),0),
            "max_chunk_len":max(lengths),
            "min_chunk_len":min(lengths),
            "sources":sources,
        }
        logger.info("="*40)
        logger.info("分开统计报告")
        for k, v in report.items():
            logger.info(f" {k}:{v}")
        
        return report


