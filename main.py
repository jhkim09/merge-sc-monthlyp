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

        rival_df = pd.read_excel(temp_input_path, sheet_name="Rival", header=26)

        sheet1.columns = [str(c).strip() for c in sheet1.columns]
        code_col = next((col for col in sheet1.columns if col.strip().lower() == 'code'), None)
        monthlyp_col = next((col for col in sheet1.columns if '월초p' in col.strip().lower()), None)
        if not code_col or not monthlyp_col:
            return {"error": "Sheet1에 'Code' 또는 '월초P' 컬럼이 없습니다."}

        sheet1 = sheet1.dropna(subset=[code_col, monthlyp_col]).copy()
        sheet1[code_col] = sheet1[code_col].apply(normalize_code)
        code_to_p = sheet1.set_index(code_col)[monthlyp_col].to_dict()

        wb = load_workbook(temp_input_path)
        if "Rival" not in wb.sheetnames:
            return {"error": "엑셀 파일에 'Rival' 시트가 없습니다."}

        ws = wb["Rival"]
        yellow_fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")

        updated_count = 0

        for idx, row in rival_df.iterrows():
            # 좌측 인원: 열 I(8), Total = O(14)
            left_code = normalize_code(row.iloc[8])
            if left_code in code_to_p:
                ws.cell(row=idx + 28, column=15).value = code_to_p[left_code]  # O열 = 15
                ws.cell(row=idx + 28, column=15).fill = yellow_fill
                updated_count += 1
                print(f"[MATCH-LEFT] Code: {left_code} → {code_to_p[left_code]}")
            else:
                print(f"[MISS-LEFT] Code: {left_code}")

            # 우측 인원: 열 R(17), Code = R(17), Total = X(24)
            right_code = normalize_code(row.iloc[17])
            if right_code in code_to_p:
                ws.cell(row=idx + 28, column=24).value = code_to_p[right_code]  # X열 = 24
                ws.cell(row=idx + 28, column=24).fill = yellow_fill
                updated_count += 1
                print(f"[MATCH-RIGHT] Code: {right_code} → {code_to_p[right_code]}")
            else:
                print(f"[MISS-RIGHT] Code: {right_code}")

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
