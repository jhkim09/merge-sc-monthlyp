from fastapi import FastAPI, File, UploadFile
from fastapi.responses import FileResponse
import pandas as pd
import shutil
import os
from uuid import uuid4

app = FastAPI()

@app.post("/merge-sc-monthlyp/")
async def merge_sc_monthlyp(file: UploadFile = File(...)):
    # 임시 파일 이름 생성
    unique_id = uuid4().hex
    temp_input_path = f"/tmp/{unique_id}_{file.filename}"
    temp_output_path = f"/tmp/merged_{unique_id}_{file.filename}"

    # 파일 저장
    with open(temp_input_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        # 엑셀 파일 읽기
        excel_data = pd.read_excel(temp_input_path, sheet_name=None)
        sheet1 = excel_data.get("Sheet1")
        sheet2 = excel_data.get("Sheet2")

        if sheet1 is None or sheet2 is None:
            return {"error": "Sheet1 또는 Sheet2가 존재하지 않습니다."}

        # 필요한 컬럼만 추출
        sheet1_filtered = sheet1[["Code", "SC", "월초P(KRW)"]]
        sheet1_filtered = sheet1_filtered.rename(columns={"월초P(KRW)": "월초P"})

        # Sheet2와 병합
        merged = pd.merge(sheet2, sheet1_filtered, on="Code", how="left")

        # 결과를 Sheet2로 저장
        with pd.ExcelWriter(temp_output_path, engine="openpyxl") as writer:
            merged.to_excel(writer, sheet_name="Sheet2", index=False)

        return FileResponse(
            temp_output_path,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            filename=f"merged_{file.filename}",
            background=lambda: cleanup_files([temp_input_path, temp_output_path])
        )

    except Exception as e:
        return {"error": str(e)}
    

def cleanup_files(paths):
    for path in paths:
        if os.path.exists(path):
            os.remove(path)
