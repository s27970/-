import pandas as pd
import numpy as np
from math import ceil
import os
import chardet

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']

def resample_dataset(input_file: str, ratio: float) -> None:
    """
    샘플링 비율에 따라 데이터셋을 샘플링하는 함수입니다.

    Parameters:
    - input_file (str): 샘플링할 파일의 경로 (xlsx, csv, json, parquet)
    - ratio (float): 샘플링 비율 (최소 클래스 샘플 수의 비율)
    """
    
    # 파일 확장자 확인
    file_extension = os.path.splitext(input_file)[1].lower()

    # 파일 확장자에 따른 파일 읽기
    if file_extension == '.xlsx':
        df = pd.read_excel(input_file)
    elif file_extension == '.csv':
        # 파일 인코딩 감지
        encoding = detect_encoding(input_file)
        try:
            df = pd.read_csv(input_file, encoding=encoding)
        except UnicodeDecodeError:
            raise ValueError(f"Unable to decode the file with detected encoding: {encoding}.")
    elif file_extension == '.json':
        df = pd.read_json(input_file)
    elif file_extension == '.parquet':
        df = pd.read_parquet(input_file)
    else:
        raise ValueError("Unsupported file format. Please upload a file in xlsx, csv, json, or parquet format.")

    # 열 이름 확인 및 공백 제거
    df.columns = df.columns.str.strip()

    if file_extension != '.json':
        # 데이터셋 확인
        if '분류' not in df.columns or 'message_tree_id' not in df.columns:
            raise KeyError("Required columns '분류' or 'message_tree_id' not found in the dataset.")

        # 분류별로 데이터프레임 분리
        class_groups = {}
        for category in df['분류'].unique():
            class_groups[category] = df[df['분류'] == category]

        # 각 분류에서 고유한 message_tree_id의 수를 셈
        class_group_counts = {category: len(group['message_tree_id'].unique()) for category, group in class_groups.items()}

        # 가장 적은 그룹 수 계산
        min_group_count = min(class_group_counts.values())

        # 지정한 비율로 제한할 최대 그룹 수 계산
        max_groups = ceil(min_group_count * ratio)

        # 각 분류에서 그룹의 수를 제한하여 샘플링
        sampled_groups = []
        for category, group in class_groups.items():
            # 각 분류에서 고유한 message_tree_id를 추출
            unique_tree_ids = group['message_tree_id'].unique()
            # 최대 그룹 수만큼 샘플링
            sampled_tree_ids = np.random.choice(unique_tree_ids, size=min(len(unique_tree_ids), max_groups), replace=False)
            # 샘플링된 message_tree_id를 가진 데이터만 추출
            sampled_group = group[group['message_tree_id'].isin(sampled_tree_ids)]
            sampled_groups.append(sampled_group)

        # 샘플링된 데이터프레임 생성
        df_resampled = pd.concat(sampled_groups)

        # 샘플링 후 데이터셋 클래스별 그룹 수 확인
        resampled_class_group_counts = df_resampled.groupby('분류')['message_tree_id'].nunique()

        # 샘플링 후 데이터셋 클래스별 그룹 수 출력
        print("\nResampled class group counts:")
        print(resampled_class_group_counts)

    else:
        # JSON 파일에서는 그룹화 및 샘플링을 수행하지 않음
        # 원래 데이터셋 클래스별 샘플 수 확인
        original_class_counts = df['분류'].value_counts()

        # 가장 적은 클래스의 샘플 수 계산
        min_class_count = original_class_counts.min()

        # 지정한 비율로 제한할 최대 샘플 수 계산
        max_samples = ceil(min_class_count * ratio)

        # 각 클래스별 샘플 수 제한
        df_resampled = df.groupby('분류').apply(lambda x: x.sample(min(len(x), max_samples))).reset_index(drop=True)

        # 샘플링 후 데이터셋 클래스별 샘플 수 확인
        resampled_class_counts = df_resampled['분류'].value_counts()

    # 입력 파일의 디렉토리와 파일 이름 추출
    input_dir = os.path.dirname(input_file)
    input_filename = os.path.basename(input_file)
    
    # 결과 파일 이름 생성 (기본 파일 이름에 '_resampled' 추가)
    output_filename = os.path.splitext(input_filename)[0] + '_resampled' + file_extension
    output_file = os.path.join(input_dir, output_filename)
    
    # 샘플링된 데이터셋 저장 (파일 형식에 따라)
    if file_extension == '.xlsx':
        df_resampled.to_excel(output_file, index=False)
    elif file_extension == '.csv':
        df_resampled.to_csv(output_file, index=False, encoding='utf-8-sig')  # 수정된 부분
    elif file_extension == '.json':
        df_resampled.to_json(output_file, orient='records', force_ascii=False, indent=4)
    elif file_extension == '.parquet':
        df_resampled.to_parquet(output_file, index=False)
    else:
        raise ValueError("Unsupported file format for saving.")

    # 샘플링 결과에 대한 간단한 설명 출력
    print("\nSampling completed successfully.")

# 함수 사용 예제
# input_file = 'C:/Users/mobla/Desktop/testing/unsersampling/테스트데이터/상담사례/oasst_lawtalk_상담사례_20240807_분류.xlsx'
# ratio = 1.5
# resample_dataset(input_file, ratio)
