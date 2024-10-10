import streamlit as st
import pandas as pd
import requests
import os
import re
import time
import zipfile
from io import BytesIO
import shutil
import csv

def safe_folder_name(name):
    """안전한 폴더 이름 생성 함수."""
    name = re.sub(r'[<>:"/\\|?*\s*\.\.{1,}]', '_', str(name))  # 문제 있는 문자와 패턴을 밑줄로 대체
    name = re.sub(r'^\.+|\.+$', '', str(name))  # 앞뒤의 점 제거
    return name[:255]  # 최대 길이 255로 제한

def clean_file_name(file):
    """파일 이름에서 확장자를 제거하고 정리하는 함수."""
    match = re.search(r'\.(\w+)', file)
    if match:
        return file[:match.start() + len(match.group(0))]
    return file

def safe_filename(file):
    """파일 이름에서 문제 있는 문자를 모두 제거하거나 밑줄로 대체하는 함수."""
    # 문제 있는 문자와 패턴을 밑줄로 대체
    return re.sub(r'[!@#$%^&*()_+\-=\[\]{}|\\:;"\'<>,/?]', '_', str(file))

def is_html(content):
    """HTML 콘텐츠 여부 확인 함수."""
    return bool(re.search(r'<html', content.decode('utf-8', errors='ignore'), re.IGNORECASE))

def initialize_logging(base_folder):
    """로그 파일 및 CSV 초기화 함수."""
    log_file_path = os.path.join(base_folder, 'log.log')
    log_csv_path = os.path.join(base_folder, 'log.csv')
    error_log_file_path = os.path.join(base_folder, 'error.log')
    error_csv_path = os.path.join(base_folder, 'error.csv')

    if not os.path.exists(base_folder):
        os.makedirs(base_folder)

    with open(log_file_path, 'w', encoding='utf-8') as log_file, \
         open(error_log_file_path, 'w', encoding='utf-8') as error_log_file:
        log_file.write('Download Log\n')
        error_log_file.write('Error Log\n')

    with open(log_csv_path, 'w', newline='', encoding='utf-8-sig') as log_csv, \
         open(error_csv_path, 'w', newline='', encoding='utf-8-sig') as error_csv:
        log_writer = csv.writer(log_csv)
        error_writer = csv.writer(error_csv)
        log_writer.writerow(['Index', 'Organization', 'Title', 'File Name', 'URL', 'Status', 'Message'])
        error_writer.writerow(['Index', 'Organization', 'Title', 'File Name', 'URL', 'Error Message'])
    
    return log_file_path, log_csv_path, error_log_file_path, error_csv_path

def download_files(df, base_download_folder):
    """데이터프레임에서 링크를 읽어와 파일을 다운로드하고 로그를 기록하는 함수."""
    log_file_path, log_csv_path, error_log_file_path, error_csv_path = initialize_logging(base_download_folder)
    
    download_count = 2087
    total_files = len(df)
    progress_bar = st.progress(0)  # 진행 상황 표시 바 생성
    status_text = st.empty()  # 진행 상황을 표시할 텍스트

    for index, row in df.iterrows():
        url = row['file_download_link']
        organization = row['organization']
        title = row['title']
        file_name = row['file_name']
        
        if pd.isna(url) or not url.strip():
            continue  # URL이 비어있거나 유효하지 않은 경우 건너뛰기

        organization_folder = safe_folder_name(organization)
        title_folder = f"{download_count+1:5d}_{safe_folder_name(title)}"
        # 파일 이름에서 <,>만 제거
        
        safe_file = clean_file_name(file_name)
        safe_file = safe_filename(safe_file)
        
        organization_folder_path = os.path.join(base_download_folder, organization_folder)
        title_folder_path = os.path.join(organization_folder_path, title_folder)
        
        if not os.path.exists(title_folder_path):
            os.makedirs(title_folder_path)

        file_path = os.path.join(title_folder_path, safe_file)

        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()  

            if is_html(response.content):
                raise ValueError("다운로드한 콘텐츠가 HTML입니다. 유효한 파일이 아닙니다.")
            
            with open(file_path, 'wb') as file:
                file.write(response.content)
            
            download_count += 1  # 다운로드 카운터 증가
        except (requests.RequestException, ValueError) as e:
            with open(error_log_file_path, 'a', encoding='utf-8') as error_log_file:
                error_log_file.write(f"{url} 다운로드 오류: {e}\n")
            continue

        # 진행 상황 표시 바 업데이트
        progress_bar.progress(download_count / total_files)
        status_text.text(f"진행 중: {download_count}/{total_files} 파일 다운로드 중")  # 진행 상황 텍스트 업데이트

    # 다운로드한 모든 파일을 포함하는 ZIP 파일 생성
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for root, _, files in os.walk(base_download_folder):
            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(file_path, base_download_folder)
                zip_file.write(file_path, relative_path)
    
    zip_buffer.seek(0)
    
    return zip_buffer

def main():
    st.title("파일 다운로드기")

    uploaded_file = st.file_uploader("Excel 또는 CSV 파일을 선택하세요", type=["xlsx", "csv"])

    if uploaded_file is not None:
        # 원본 파일 이름에서 확장자 제거
        original_file_name = os.path.splitext(uploaded_file.name)[0]
        
        # 기본 다운로드 폴더 생성
        base_download_folder = os.path.join(os.getcwd(), "downloads")
        
        # 다운로드 폴더를 지우기 전에 확인
        if os.path.exists(base_download_folder):
            shutil.rmtree(base_download_folder)  # 기존 폴더 삭제

        # 파일 타입에 따라 데이터프레임 읽기
        if uploaded_file.name.endswith('.xlsx'):
            df = pd.read_excel(uploaded_file)
        elif uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)

        # 파일 다운로드 및 ZIP 버퍼 생성
        zip_buffer = download_files(df, base_download_folder)
        
        # 원본 파일 이름을 기반으로 ZIP 파일 이름 생성
        zip_file_name = f"{original_file_name}.zip"
        
        st.write(f"다운로드 완료! {len(df)} 개의 파일 처리됨.")
        
        # 동적 파일 이름으로 ZIP 파일 다운로드 제공
        st.download_button(
            label="모든 파일을 ZIP으로 다운로드",
            data=zip_buffer,
            file_name=zip_file_name,
            mime="application/zip"
        )

if __name__ == "__main__":
    main()
