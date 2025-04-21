from fastapi import FastAPI, File, UploadFile, BackgroundTasks
from fastapi.responses import FileResponse
import pandas as pd
import shutil
import os
from uuid import uuid4
from openpyxl import load_workbook
from openpyxl.styles import PatternFill

app = FastAPI()

@app.post("/merge-sc-monthlyp/")
async def merge_sc_monthlyp(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    unique_id = uuid4().hex
    temp_input_path = f"/tmp/{unique_id}_{file.filename}"
    temp_output_path = f"/tmp/merged_{unique_id}_{file.filename}"

    with open(temp_input_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        excel_data = pd.read_excel(temp_input_path, sheet_name=None)
        sheet1 = excel_data.get("Sheet1")
        rival_df = excel_data.get("Rival")

        if sheet1 is None or rival_df is None:
            return {"error": "Sheet1 또는 Rival 시트가 존재하지 않습니다."}

        sheet1.columns = [str(c).strip() for c in sheet1.columns]
        code_col = next((col for col in sheet1.columns if '코드' in col or 'Code' in col), None)
        if not code_col:
            return {"error": "Sheet1에 'Code' 또는 '코드' 컬럼이 없습니다."}

        sheet1 = sheet1.dropna(subset=[code_col, "월초P(KRW)"]).copy()
        sheet1[code_col] = sheet1[code_col].astype(str)
        code_to_p = sheet1.set_index(code_col)["월초P(KRW)"].to_dict()

        rival_df.columns = [str(c).strip() for c in rival_df.columns]
        rival_code_col = next((col for col in rival_df.columns if '코드' in col or 'Code' in col), None)
        if not rival_code_col:
            return {"error": "Rival 시트에 '코드' 또는 'Code' 컬럼이 없습니다."}

        wb = load_workbook(temp_input_path)
        if "Rival" not in wb.sheetnames:
            return {"error": "엑셀 파일에 'Rival' 시트가 없습니다."}

        ws = wb["Rival"]
        yellow_fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")

        for idx, row in rival_df.fillna("").iterrows():
            row_values = row.astype(str).tolist()
            target_code = str(row[rival_code_col]).strip()

            if any("total" in str(v).strip().lower() for v in row_values):
                if target_code in code_to_p:
                    print(f"✔️ 코드 매칭: {target_code} → {code_to_p[target_code]}")
                    for col in rival_df.columns:
                        if str(row[col]).strip().lower() == "total":
                            col_index = rival_df.columns.get_loc(col) + 1
                            excel_row = idx + 2
                            value_to_set = code_to_p[target_code]
                            ws.cell(row=excel_row, column=col_index).value = value_to_set
                            ws.cell(row=excel_row, column=col_index).fill = yellow_fill
                            break
                else:
                    print(f"❌ 코드 없음: {target_code}")

        wb.save(temp_output_path)
        background_tasks.add_task(cleanup_files, [temp_input_path, temp_output_path])

        return FileResponse(
            temp_output_path,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            filename=f"merged_{file.filename}",
            background=background_tasks
        )

    except Exception as e:
        return {"error": str(e)}


def cleanup_files(paths):
    for path in paths:
        if os.path.exists(path):
            os.remove(path)
