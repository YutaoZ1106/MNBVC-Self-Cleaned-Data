import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import hashlib
import re
import os
import shutil
from bs4 import BeautifulSoup
from markdown2 import markdown

#读取并转换md
def parse_markdown_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            markdown_content = file.read()
            html_content = markdown(markdown_content) # 将Markdown转换为HTML
        return html_content
    except Exception as e:
        return None

    
def extract_img_src_from_paragraph(paragraph):
    # 检查这个段落是否包含<img>标签
    img_tag = paragraph.find('img')
    if img_tag and 'src' in img_tag.attrs:
        # 返回<img>标签的src属性值
        if not re.match(r'^(http:\/\/|https:\/\/).*', img_tag['src']):
            return img_tag['src']
    return None  # 如果没有找到符合条件的<img>标签，返回None

def calculate_md5(text):
    md5_hash = hashlib.md5()
    md5_hash.update(text.encode('utf-8'))
    return md5_hash.hexdigest()


def parse_text_parquet(text, fileId, pageId, blockId, date, 
                        extra, fileName, dir):
    file = pd.DataFrame()
    file['md5'] = calculate_md5(text=text)
    file['fileId'] = fileId
    file['pageId'] = pageId
    file['blockId'] = blockId
    file['text'] = text
    file['image'] = None
    file['time'] = date
    file['dataType'] = '文本'
    file['boundingBox'] = []
    file['extra'] = extra
    table = pa.Table.from_pandas(file)
    path = os.path.join(dir, fileName)
    pq.write_table(table, path)

def parse_img_parquet(location, fileId, pageId, blockId, date,
                        extra, fileName, dir):
    #需要在不同的电脑上更改current dir
    current_dir = 'e:/DATA CLEANING'
    full_path = current_dir + location
    binary_data = None
    try:
        with open(full_path, 'rb') as file:
            binary_data = file.read()
    except Exception as e:
        binary_data = None
        
    file = pd.DataFrame()
    file['md5'] = binary_data
    file['fileId'] = fileId
    file['pageId'] = pageId
    file['blockId'] = blockId
    file['text'] = None
    file['image'] = binary_data
    file['time'] = date
    file['dataType'] = '图片'
    file['boundingBox'] = []
    file['extra'] = extra
    table = pa.Table.from_pandas(file)
    path = os.path.join(dir, fileName)
    pq.write_table(table, path)

def file_writing(html_text, dir):
    soup = BeautifulSoup(html_text, 'html.parser')
    
    # 提取第一个段落
    content_info = soup.find('p').text
    
    # 提取fileId
    fileId = None
    pattern = r'^id: (\d+)$'
    match = re.search(pattern, content_info, re.MULTILINE)
    if match:
        fileId = match.group(1)
    else:
        fileId = None
    
    #默认pageId是0
    pageId = 0
    
    #初始blockId为0
    blockId = 0
    
    #提取date
    pattern = r"date:\s*'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})'"
    match = re.search(pattern, content_info)
    if match:
        date = match.group(1)  # 提取日期时间字符串
    else:
        date = None
    
    remaining_paragraphs = soup.find_all('p')[1:]
    current_text = ""
    for p in remaining_paragraphs:
        location = extract_img_src_from_paragraph(p)
        if location is None:
            current_text = current_text + p.text
            continue
        parse_text_parquet(text=current_text, fileId=fileId,
                            pageId=pageId, blockId=blockId, date=date,
                            extra= content_info,
                            fileName=str(fileId)+'-'+str(blockId)+'.parquet',
                            dir=dir)
        blockId += 1
        parse_img_parquet(location=location, fileId=fileId,
                            pageId=pageId, blockId=blockId, date=date,
                            extra= content_info,
                            fileName=str(fileId)+'-'+str(blockId)+'.parquet',
                            dir=dir)
        blockId += 1
        current_text = ""
    parse_text_parquet(text=current_text, fileId=fileId,
                        pageId=pageId, blockId=blockId, date=date,
                        extra= content_info,
                        fileName=str(fileId)+'-'+str(blockId)+'.parquet',
                        dir=dir)
    blockId += 1

def visit_directory(src, dst):
    """
    复制并处理src目录到dst目录，包括src下的所有子目录和文件。
    """
    # 遍历源目录
    for root, dirs, files in os.walk(src):
        # 计算目标目录路径
        dst_dir = root.replace(src, dst, 1)
        
        # 如果目标目录不存在，创建它
        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir)
        
        # 对每个文件执行处理然后保存到新目录
        for file in files:
            src_file = os.path.join(root, file)
            #dst_file = os.path.join(dst_dir, file)
            html_content = parse_markdown_file(src_file)
            if html_content is None:
                continue
            file_writing(html_content, dst_dir)
        
        # 如果需要复制空目录（可选）
        for dir in dirs:
            src_dir = os.path.join(root, dir)
            dst_dir = os.path.join(root.replace(src, dst, 1), dir)
            if not os.path.exists(dst_dir):
                os.makedirs(dst_dir)
visit_directory('e:/DATA CLEANING/content', 'e:/DATA CLEANING/result')
#markdown_file = '114.md'
#html_content = parse_markdown_file(markdown_file)
#print(html_content)
#file_writing(html_content)