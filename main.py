from fastapi import FastAPI, File, UploadFile, BackgroundTasks
from fastapi.responses import FileResponse
import pandas as pd
import shutil
import os
from uuid import uuid4

app = FastAPI()

@app.post("/merge-sc-monthlyp/")
async def merge_sc_monthlyp(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
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
        rival = excel_data.get("Rival")

        if sheet1 is None or rival is None:
            return {"error": "Sheet1 또는 Rival 시트가 존재하지 않습니다."}

        # Code, 월초P 정보 딕셔너리로 구성
        sheet1 = sheet1.dropna(subset=["Code", "월초P(KRW)"])
        sheet1["Code"] = sheet1["Code"].astype(str)
        code_to_p = sheet1.set_index("Code")["월초P(KRW)"].to_dict()

        # Rival 시트 처리
        rival_filled = rival.fillna("")
        updated_rows = 0
        codes = []

        for idx, row in rival_filled.iterrows():
            row_values = row.astype(str).tolist()

            if any("본부" in v for v in row_values):
                codes = []  # 새로운 인물 시작

            # 코드 수집
            for val in row_values:
                if val.strip().isdigit():
                    codes.append(val.strip())

            if any("Total" in v for v in row_values) and codes:
                target_code = codes[0]  # 첫 번째 코드만 기준으로 사용
                if target_code in code_to_p:
                    for col in rival.columns:
                        if str(row[col]).strip() == "Total":
                            rival.at[idx, col] = code_to_p[target_code]
                            updated_rows += 1
                            break
                codes = []  # 다음 사람 준비

        # 저장
        with pd.ExcelWriter(temp_output_path, engine="openpyxl") as writer:
            sheet1.to_excel(writer, sheet_name="Sheet1", index=False)
            rival.to_excel(writer, sheet_name="Rival", index=False)

        # 삭제 예약
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
