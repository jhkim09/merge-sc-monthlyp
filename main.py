from fastapi import FastAPI, File, UploadFile, BackgroundTasks
from fastapi.responses import StreamingResponse
import pandas as pd
import shutil
import os
from uuid import uuid4
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from io import BytesIO
from urllib.parse import quote

app = FastAPI()

def normalize_code(value):
    try:
        return str(int(float(value))).strip()
    except:
        return str(value).strip()

@app.post("/merge-sc-monthlyp/")
async def merge_sc_monthlyp(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    unique_id = uuid4().hex
    temp_input_path = f"/tmp/{unique_id}_{file.filename}"

    with open(temp_input_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        excel_data = pd.read_excel(temp_input_path, sheet_name=None)
        sheet1 = excel_data.get("Sheet1")
        if sheet1 is None:
            return {"error": "Sheet1 시트를 찾을 수 없습니다."}

        # Rival 시트에서 실제 컬럼 헤더가 있는 27행을 기준으로 로딩
        rival_df = pd.read_excel(temp_input_path, sheet_name="Rival", header=26)

        sheet1.columns = [str(c).strip() for c in sheet1.columns]
        code_col = next((col for col in sheet1.columns if col.strip().lower() == 'code'), None)
        if not code_col:
            return {"error": "Sheet1에 정확한 'Code' 컬럼이 없습니다."}

        sheet1 = sheet1.dropna(subset=[code_col, "월초P(KRW)"]).copy()
        sheet1[code_col] = sheet1[code_col].apply(normalize_code)
        code_to_p = sheet1.set_index(code_col)["월초P(KRW)"].to_dict()

        wb = load_workbook(temp_input_path)
        if "Rival" not in wb.sheetnames:
            return {"error": "엑셀 파일에 'Rival' 시트가 없습니다."}

        ws = wb["Rival"]
        yellow_fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")

        updated_count = 0

        rival_columns = rival_df.columns.tolist()
        for idx, row in rival_df.iterrows():
            for i in range(2):
                try:
                    start = i * (len(rival_columns) // 2)
                    end = (i + 1) * (len(rival_columns) // 2)
                    person_data = row.iloc[start:end]
                    person_data.index = rival_columns[start:end]  # 컬럼명 붙이기

                    print(f"[DEBUG] Person {i} columns: {rival_columns[start:end]}")

                    # 코드 및 Total 컬럼 자동 탐지
                    code_key = next((k for k in person_data.keys() if '코드' in str(k).lower() or 'code' in str(k).lower()), None)
                    total_key = next((k for k in person_data.keys() if 'total' in str(k).lower()), None)

                    code = normalize_code(person_data.get(code_key, "")) if code_key else ""
                    print(f"[DEBUG] Code value: '{code}'")
                    print(f"[DEBUG] Is in code_to_p: {code in code_to_p}")
                    print(f"[DEBUG] Total key: {total_key}")

                    if code and code in code_to_p and total_key:
                        col_index = start + list(rival_columns[start:end]).index(total_key)
                        excel_row = idx + 28  # 실제 엑셀 기준 행 번호 보정
                        excel_col = col_index + 1
                        ws.cell(row=excel_row, column=excel_col).value = code_to_p[code]
                        ws.cell(row=excel_row, column=excel_col).fill = yellow_fill
                        updated_count += 1
                        print(f"[MATCH] Code: '{code}' → {code_to_p[code]}")
                    else:
                        print(f"[MISS]  Code not found or missing Total column: '{code}'")
                except Exception as e:
                    print(f"[ERROR] Row {idx}, Person {i}: {e}")

        print(f"[RESULT] Total updated cells: {updated_count}")

        output = BytesIO()
        wb.save(output)
        output.seek(0)

        background_tasks.add_task(cleanup_files, [temp_input_path])

        safe_filename = quote(f"merged_{file.filename}")
        return StreamingResponse(
            output,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition": f"attachment; filename*=UTF-8''{safe_filename}"}
        )

    except Exception as e:
        print(f"[ERROR] {str(e)}")
        return {"error": str(e)}


def cleanup_files(paths):
    for path in paths:
        if os.path.exists(path):
            os.remove(path)
