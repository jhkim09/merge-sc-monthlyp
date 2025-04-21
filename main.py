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

        # Rival 시트에서 컬럼 행 자동 탐색
        temp_df = pd.read_excel(temp_input_path, sheet_name="Rival", header=None)
        header_row_index = None
        for i, row in temp_df.iterrows():
            if any(str(cell).strip().lower() in ["코드", "code"] for cell in row):
                header_row_index = i
                break

        if header_row_index is None:
            return {"error": "Rival 시트에서 '코드' 또는 'Code' 컬럼을 찾을 수 없습니다."}

        rival_df = pd.read_excel(temp_input_path, sheet_name="Rival", header=header_row_index)

        sheet1.columns = [str(c).strip() for c in sheet1.columns]
        # 정확히 'Code' 컬럼만 지정
        code_col = "Code"
        if code_col not in sheet1.columns:
            return {"error": "Sheet1에 'Code' 컬럼이 없습니다."}

        sheet1 = sheet1.dropna(subset=[code_col, "월초P(KRW)"]).copy()
        sheet1[code_col] = sheet1[code_col].apply(normalize_code)
        code_to_p = sheet1.set_index(code_col)["월초P(KRW)"].to_dict()

        print(f"[DEBUG] code_to_p keys (sample): {list(code_to_p.keys())[:10]}")

        rival_df.columns = [str(c).strip() for c in rival_df.columns]
        rival_code_col = next((col for col in rival_df.columns if '코드' in col or 'Code' in col), None)
        if not rival_code_col:
            return {"error": "Rival 시트에 '코드' 또는 'Code' 컬럼이 없습니다."}

        wb = load_workbook(temp_input_path)
        if "Rival" not in wb.sheetnames:
            return {"error": "엑셀 파일에 'Rival' 시트가 없습니다."}

        ws = wb["Rival"]
        yellow_fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")

        updated_count = 0

        for idx, row in rival_df.fillna("").iterrows():
            row_values = row.astype(str).tolist()
            target_code = normalize_code(row[rival_code_col])
            print(f"[DEBUG] target_code: {target_code}")

            if any("total" in str(v).strip().lower() for v in row_values):
                if target_code in code_to_p:
                    print(f"[MATCH] Code: {target_code} → {code_to_p[target_code]}")
                    for col in rival_df.columns:
                        if str(row[col]).strip().lower() == "total":
                            col_index = rival_df.columns.get_loc(col) + 1
                            excel_row = idx + header_row_index + 2
                            value_to_set = code_to_p[target_code]
                            ws.cell(row=excel_row, column=col_index).value = value_to_set
                            ws.cell(row=excel_row, column=col_index).fill = yellow_fill
                            updated_count += 1
                            break
                else:
                    print(f"[MISS]  Code not found: {target_code}")

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
