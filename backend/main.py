import os
import cv2
from fastapi import Depends,FastAPI,HTTPException,Form,UploadFile,Response
import numpy as np
from sqlalchemy.orm import Session
from fastapi.middleware.cors import CORSMiddleware
from typing import Annotated
from io import StringIO
from src import crud, models, schemas
from src.database import SessionLocal, engine

import asyncio

import PyPDF2

from fastapi.staticfiles import StaticFiles

from PyPDF2 import PdfReader, PdfWriter
import json
from fastapi.responses import JSONResponse, StreamingResponse

models.Base.metadata.create_all(bind=engine)
database_semaphore = asyncio.Semaphore(value=1)

app = FastAPI()
app.mount("/pdf", StaticFiles(directory="upload_pdf"), name="pdf")
app.mount("/image", StaticFiles(directory="upload_image"), name="image")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Replace "" with the origins you want to allow
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],  # Add other HTTP methods as needed
    allow_headers=["*"],  # Allow all headers
)

# ----------------------------------------------------------------------------------------

@app.get("/get_shell_Menus",response_model=list[schemas.shell_Menus], description="Retrieve All the Shell Menus")
async def get_shell_Menus(db: Session = Depends(get_db)):
    db_shell_Menus = crud.get_shell_Menus(db)
    if db_shell_Menus is None:
        raise HTTPException(status_code=404, detail="User not found")
    return db_shell_Menus

# ----------------------------------------------------------------------------------------

@app.post("/insert_user_Role/", response_model=str, description='User Role is Inserting With Input JSON Type')
def insert_user_Role(user_role:schemas.user_Roles,db:Session = Depends(get_db)):   
    return crud.insert_user_Role(db, user_role)

@app.get("/get_user_Role",response_model=list[schemas.user_Roles_Full_Detail], description='Getting all the User Role Details')
async def get_user_Role(db: Session = Depends(get_db)):
    db_user_Role = crud.get_user_Role(db)
    if db_user_Role is None:
        raise HTTPException(status_code=404, detail="Role not found")
    return db_user_Role

@app.delete("/delete_user_Role/", response_model=list[schemas.user_Roles_Full_Detail], description='Deleting a Particular User Role')
def delete_user_Role(role_id:Annotated[int, Form()],db:Session = Depends(get_db)):   
    return crud.delete_user_Role(db, role_id)

@app.put("/update_user_Role/", response_model=list[schemas.user_Roles_Full_Detail], description='Update a Particular User Role')
def update_user_Role(role_id:Annotated[int, Form()],update_role_name:Annotated[str, Form()],db:Session = Depends(get_db)):   
    return crud.update_user_Role(db, role_id, update_role_name)

# ----------------------------------------------------------------------------------------

@app.post("/insert_Demography/", response_model=str)
def insert_Demography(shell_Demography:schemas.shell_Demography,db:Session = Depends(get_db)):   
    return crud.insert_Demography(db,shell_Demography)

@app.get("/get_Demography",response_model=list[schemas.shell_Demography_Full_Detail])
async def get_Demography(db: Session = Depends(get_db)):
    db_get_Demography = crud.get_Demography(db)
    if db_get_Demography is None:
        raise HTTPException(status_code=404, detail="Demography not found")
    return db_get_Demography

@app.delete("/delete_Demography/", response_model=list[schemas.shell_Demography_Full_Detail])
def delete_Demography(demography_id:Annotated[int, Form()],db:Session = Depends(get_db)):   
    return crud.delete_Demography(db, demography_id)

@app.put("/update_Demography/", response_model=list[schemas.shell_Demography_Full_Detail])
def update_Demography(demography_id:Annotated[int, Form()],demography_name:Annotated[str, Form()],db:Session = Depends(get_db)):   
    return crud.update_Demography(db, demography_id, demography_name)

# ----------------------------------------------------------------------------------------

@app.post("/insert_datasource_type",response_model=str)
def insert_datasource_type(shell_DataSource:schemas.shell_DataSource,db:Session = Depends(get_db)):
    return crud.insert_datasource_type(db,shell_DataSource)

@app.get("/get_dataSource_type",response_model=list[schemas.shell_DataSource_Full_Detail])
def get_dataSource_type(db:Session = Depends(get_db)):
    return crud.get_dataSource_type(db)

@app.delete("/delete_datasource_type",response_model=list[schemas.shell_DataSource_Full_Detail])
def delete_datasource_type(datasource_id:Annotated[int, Form()],db:Session=Depends(get_db)):
    return crud.delete_dataSource_type(db,datasource_id)

@app.put("/update_data_source_type",response_model=list[schemas.shell_DataSource_Full_Detail])
def update_data_source_type(datasource_id:Annotated[int, Form()],datasource_type:Annotated[str, Form()],db:Session = Depends(get_db)):
    return crud.update_dataSource_type(db,datasource_id,datasource_type)

# ----------------------------------------------------------------------------------------

@app.post("/insert_annotate_marker",response_model=str)
def insert_annotate_marker(annotate_marker_schema:schemas.shell_annotateMarker,db:Session=Depends(get_db)):
    return crud.insert_annotate_marker(db,annotate_marker_schema)

@app.get("/get_annotation_marker",response_model=list[schemas.shell_annotateMarker_Full_Detail])
async def get_annotation_marker(db:Session = Depends(get_db)):
    return crud.get_annotate_marker(db)

@app.delete("/delete_annotate_marker",response_model=list[schemas.shell_annotateMarker_Full_Detail])
def delete_annotate_marker(annotate_marker_id:Annotated[int, Form()],db:Session=Depends(get_db)):
    return crud.delete_annotate_marker(db,annotate_marker_id)

@app.put("/update_annotate_marker",response_model=list[schemas.shell_annotateMarker_Full_Detail])
async def update_annotate_marker(annotate_marker_schema : schemas.shell_annotateMarker_Full_Detail,db:Session=Depends(get_db)):
    return crud.update_annotate_marker(db,annotate_marker_schema)

# ----------------------------------------------------------------------------------------

@app.post("/insert_menu_permission",response_model=str)
def insert_menu_permission(menu_permission_schema:schemas.shell_menu_permission,db:Session=Depends(get_db)):
    return crud.inser_menu_permission(db,menu_permission_schema)

@app.get("/get_menu_permission",response_model=list)
def insert_menu_permission(db:Session=Depends(get_db)):
    return crud.get_menu_permission(db)

# ----------------------------------------------------------------------------------------

@app.post("/insert_user_details",response_model=list[schemas.shell_User_table])
def insert_user_details(user_detail_schema:schemas.shell_User_table_without_id,db:Session=Depends(get_db)):
    return crud.insert_user_details(db,user_detail_schema)

@app.post("/insert_user_details_bulk",response_model=list[schemas.shell_User_table])
async def insert_user_details(file:UploadFile,db:Session=Depends(get_db)):
    return crud.insert_user_details_bulk(db,file)

# ----------------------------------------------------------------------------------------

@app.post("/project_creation",response_model=str)
async def project_creation(pj_anno:Annotated[str,Form()],project_name:Annotated[str,Form()],project_demography:Annotated[int,Form()],project_input_type:Annotated[int,Form()],file:UploadFile,db:Session=Depends(get_db)):
    return crud.insert_project_table(db,project_name,project_demography,project_input_type,file,pj_anno)

@app.get("/get_project",response_model=list)
async def get_project(db:Session = Depends(get_db)):
    return crud.get_project(db)

@app.post("/get_project_user_list",response_model=list)
async def get_project(project_id:Annotated[int,Form()],db:Session = Depends(get_db)):
    return crud.get_project_user_list(db,project_id)

@app.put("/update_project",response_model=str)
async def update_project(project_id:Annotated[int,Form()],project_name:Annotated[str,Form()],project_demography:Annotated[int,Form()],project_dataSource:Annotated[int,Form()],db:Session = Depends(get_db)):
    return crud.update_project(db,project_id,project_name,project_demography,project_dataSource)

@app.post("/add_more_users_to_project",response_model=str)
async def add_more_users_to_project(project_id:Annotated[int,Form()],file:UploadFile,db:Session = Depends(get_db)):
    return crud.add_more_users_to_project(db,project_id,file)

@app.post("/add_more_annotation_to_project",response_model=str)
async def add_more_annotation_to_project(project_id:Annotated[int,Form()],annotated_str:Annotated[str,Form()],db:Session = Depends(get_db)):
    return crud.add_more_annotation_to_project(db,project_id,annotated_str)

# ----------------------------------------------------------------------------------------

@app.post("/cycle_creation",response_model=str)
def cycle_creation(cycle_schema:schemas.shell_Project_Cycle_Creation,db:Session=Depends(get_db)):
    return crud.insert_cycle_creation(db,cycle_schema)

@app.post("/get_cycle",response_model=list)
def get_cycle(project_id:Annotated[int,Form()],db:Session=Depends(get_db)):
    return crud.get_list_of_cycles(db,project_id)

@app.put("/update_cycle",response_model=str)
def update_cycle(cycle_updated_schema:schemas.cycle_update_schema,db:Session=Depends(get_db)):
    return crud.update_cycle(db,cycle_updated_schema)

@app.delete("/delete_cycle",response_model=str)
def delete_cycle(cycle_id:Annotated[int,Form()],db:Session=Depends(get_db)):
    return crud.delete_cycle(db,cycle_id)

# ----------------------------------------------------------------------------------------

@app.post("/tracking_sheet_upload",response_model=str)
def tracking_sheet_upload(project_id:Annotated[int,Form()],cycle_id:Annotated[int,Form()],uploaded_by:Annotated[int,Form()],file:UploadFile,db:Session=Depends(get_db)):
    return crud.tracking_sheet_upload(db,file,project_id,cycle_id,uploaded_by)

# ----------------------------------------------------------------------------------------

@app.post("/store_image_upload",response_model=str)
async def store_image_upload(project_id:Annotated[int,Form()],cycle_id:Annotated[int,Form()],uploaded_by:Annotated[int,Form()],file:UploadFile,db:Session=Depends(get_db)):
    return await crud.store_image_upload(db,file,project_id,cycle_id,uploaded_by)

@app.get("/progress/{session_id}")
async def progress(session_id: str):
    return StreamingResponse(crud.event_generator(session_id), media_type="text/event-stream")

# ----------------------------------------------------------------------------------------

@app.post("/planogram_upload",response_model=str)
async def planogram_upload(project_id:Annotated[int,Form()],cycle_id:Annotated[int,Form()],uploaded_by:Annotated[int,Form()],file:UploadFile,db:Session=Depends(get_db)):
    return await crud.planogram_upload(db,file,project_id,cycle_id,uploaded_by)

# ----------------------------------------------------------------------------------------

@app.get("/get_all_project",response_model=list)
def get_all_project(db:Session=Depends(get_db)):
    return crud.get_all_project(db)

# ----------------------------------------------------------------------------------------

@app.post("/get_qa",response_model=list)
async def get_qa(emp_id:Annotated[str,Form()],db:Session=Depends(get_db)):
     async with database_semaphore:
          list_data = []
          list_data = crud.get_question_answer(db,emp_id)
          await asyncio.sleep(1)
          return list_data

# ----------------------------------------------------------------------------------------
    
@app.post("/get_upc_text")
def extract_text_from_pdf(pdf_path:Annotated[str,Form()]):
    text = ""
    with open(pdf_path, "rb") as file:
        pdf_reader = PyPDF2.PdfReader(file)
        num_pages = len(pdf_reader.pages)
        for page_num in range(num_pages):
            page = pdf_reader.pages[page_num]
            text += page.extract_text()
    lines = text.strip().split('\n')
    rows = []
    read_data = False
    temp = ""
    for line in lines:
        if "Location_ID" in line:
            read_data = True
            continue

        if "Page" in line:
            read_data = False
            continue

        if not line.strip() or not read_data:
            continue
    
        cells = line.strip().split()
        
        if "Fixel_ID" in cells:
            temp = cells[1]
            continue

        row = {
            # "Shelf_no":temp.split("/")[0],
            # "row_no":temp.split("/")[1],
            # "Location_ID": cells[-1],
            # "PLU": cells[-2],
            "UPC": cells[-3],
            "Product Name": ' '.join(cells[1:-3]),
            # "Distributor": cells[0]
        }
        rows.append(row)
    
    return rows

# ----------------------------------------------------------------------------------------

def get_pdf_page_size(input_path):
    with open(input_path, 'rb') as input_file:
        pdf_reader = PdfReader(input_file)
        first_page = pdf_reader.pages[0]
        mediabox = first_page.mediabox
        page_width = mediabox.upper_right[0] - mediabox.lower_left[0]
        page_height = mediabox.upper_right[1] - mediabox.lower_left[1]
        return page_width, page_height

@app.post("/get_axis")
def generate_pdf(axis_points: Annotated[str, Form()],input_path:Annotated[str,Form()],production_id:Annotated[int,Form()],eight_digit_store_number:Annotated[str,Form()],db:Session=Depends(get_db)):
    x_axis_y_axis = axis_points
    input_path = input_path
    output_path = input_path[:-4]+'_'+eight_digit_store_number+'_output.pdf'
    x_axis_y_axis = x_axis_y_axis.replace("'", '"')
    marker_positions =  json.loads(x_axis_y_axis)
    html_width = 0
    html_height = 0
    pdf_width, pdf_height = get_pdf_page_size(input_path)
    area_x = 50
    area_y = 50
    area_width = 1000
    area_height = 700
    crud.add_markers_to_pdf(db,input_path, output_path, marker_positions, html_width, html_height, pdf_width, pdf_height, area_x, area_y, area_width, area_height,production_id)
    return "Success"

# ----------------------------------------------------------------------------------------

@app.post("/add_upc")
def add_upc(production_id:Annotated[int,Form()],upc_json:Annotated[str,Form()],db:Session=Depends(get_db)):
    crud.add_upc(db,upc_json,production_id)
    return "Success"

# ----------------------------------------------------------------------------------------

@app.post("/add_count")
def add_count(production_id:Annotated[int,Form()],green_count:Annotated[str,Form()],blue_count:Annotated[str,Form()],red_count:Annotated[str,Form()],db:Session=Depends(get_db)):
    crud.add_count(db,production_id,green_count,blue_count,red_count)
    return "Success"

# ----------------------------------------------------------------------------------------

@app.post("/single_production_completed")
def single_production_completed(production_id:Annotated[int,Form()],production_res:Annotated[str,Form()],db:Session=Depends(get_db)):
    crud.single_production_completed(db,production_id,production_res)
    return "Success"

# -------------------------------------AUDIT---------------------------------------------------

@app.post("/get_audit_qa",response_model=list)
async def get_qa(emp_id:Annotated[str,Form()],db:Session=Depends(get_db)):
     async with database_semaphore:
          list_data = []
          list_data = crud.get_audit_question_answer(db,emp_id)
          await asyncio.sleep(1)
          return list_data

@app.post("/get_productioned_upc")
def get_productioned_upc(production_id:Annotated[int,Form()],db:Session=Depends(get_db)):
    return crud.get_productioned_upc(db,production_id)

@app.post("/add_audit_upc")
def add_audit_upc(production_id:Annotated[int,Form()],upc_json:Annotated[str,Form()],db:Session=Depends(get_db)):
    crud.add_audit_upc(db,upc_json,production_id)
    return "Success"

@app.post("/add_audit_count")
def add_audit_count(production_id:Annotated[int,Form()],green_count:Annotated[str,Form()],blue_count:Annotated[str,Form()],red_count:Annotated[str,Form()],db:Session=Depends(get_db)):
    crud.add_audit_count(db,production_id,green_count,blue_count,red_count)
    return "Success"

@app.post("/single_audit_completed")
def single_audit_completed(production_id:Annotated[int,Form()],production_res:Annotated[str,Form()],db:Session=Depends(get_db)):
    crud.single_audit_completed(db,production_id,production_res)
    return "Success"

def get_pdf_page_size_audit(input_path):
    with open(input_path, 'rb') as input_file:
        pdf_reader = PdfReader(input_file)
        first_page = pdf_reader.pages[0]
        mediabox = first_page.mediabox
        page_width = mediabox.upper_right[0] - mediabox.lower_left[0]
        page_height = mediabox.upper_right[1] - mediabox.lower_left[1]
        return page_width, page_height

@app.post("/get_axis_audit")
def get_axis_audit(axis_points: Annotated[str, Form()],input_path:Annotated[str,Form()],production_id:Annotated[int,Form()],eight_digit_store_number:Annotated[str,Form()],db:Session=Depends(get_db)):
    x_axis_y_axis = axis_points
    input_path = input_path
    output_path = input_path[:-4]+'_'+eight_digit_store_number+'_audit.pdf'
    x_axis_y_axis = x_axis_y_axis.replace("'", '"')
    marker_positions =  json.loads(x_axis_y_axis)
    html_width = 0
    html_height = 0
    pdf_width, pdf_height = get_pdf_page_size_audit(input_path)
    area_x = 50
    area_y = 50
    area_width = 1000
    area_height = 700
    crud.add_markers_to_pdf_for_audit(db,input_path, output_path, marker_positions, html_width, html_height, pdf_width, pdf_height, area_x, area_y, area_width, area_height,production_id)
    return "Success"

@app.post("/get_axis_points_for_audit")
def get_axis_points_for_audit(production_id:Annotated[int,Form()],db:Session=Depends(get_db)):
    return crud.get_axis_points_for_audit(db,production_id)
# -------------------------------------END OF AUDIT---------------------------------------------------

@app.post("/login")
def login(employee_id:Annotated[str,Form()],db: Session = Depends(get_db)):
    return crud.login(db,employee_id)


@app.post("/get_menu_permission_for_role",response_model=list)
def get_menu_permission_for_role(role_name:Annotated[str,Form()],db:Session=Depends(get_db)):
    return crud.get_menu_permission_for_role(db,role_name)


@app.post("/production_skip")
def production_skip(production_id:Annotated[int,Form()],db:Session=Depends(get_db)):
    crud.skip_production(db,production_id)
    return "Success"

@app.post("/audit_skip")
def audit_skip(production_id:Annotated[int,Form()],db:Session=Depends(get_db)):
    crud.skip_audit(db,production_id)
    return "Success"

@app.get("/planogram_type_category")
def planogram_type_category(db:Session=Depends(get_db)):
    return crud.planogram_type_category(db)

@app.post("/employee_specialisation")
def employee_specialisation(emp_id_str:Annotated[str,Form()],specialization_str:Annotated[str,Form()],db:Session=Depends(get_db)):
    return crud.employee_specialisation(db,emp_id_str,specialization_str)

@app.get("/employee_list")
def employee_list(db:Session=Depends(get_db)):
    return crud.employee_list(db)

@app.post("/retrieve_picked")
def retrieve_picked(project_id:Annotated[int,Form()],cycle_id:Annotated[int,Form()],retrieve_type:Annotated[str,Form()],db:Session=Depends(get_db)):
    return crud.retrieve_picked(db,project_id,cycle_id,retrieve_type)

@app.post("/reallocation")
def reallocation(production_id:Annotated[str,Form()],user_id:Annotated[int,Form()],reallocate_type:Annotated[str,Form()],db:Session=Depends(get_db)):
    return crud.reallocation(db,production_id,user_id,reallocate_type)

@app.post("/production_report")
def production_report(Project_id:Annotated[int,Form()],Cycle_id:Annotated[int,Form()],from_date:Annotated[str,Form()],to_date:Annotated[str,Form()],db:Session=Depends(get_db)):
    df = crud.production_report(db,Project_id,Cycle_id,from_date,to_date)
    csv_writer = StringIO()
    df.to_csv(csv_writer, index=False)
    csv_writer.seek(0)
    # headers = {
    #     'Content-Disposition': 'attachment; filename="data.csv"',
    #     'Content-Type': 'text/csv'
    # }
    # return Response(content=csv_writer.getvalue(), headers=headers)
    response = Response(content=csv_writer.getvalue(), media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=data.csv"
    response.headers["Content-Type"] = "text/csv"
    return response

@app.post("/production_report_zip_file")
def production_report_zip_file(Project_id:Annotated[int,Form()],Cycle_id:Annotated[int,Form()],from_date:Annotated[str,Form()],to_date:Annotated[str,Form()],db:Session=Depends(get_db)):
    return crud.production_report_zip_file(db,Project_id,Cycle_id,from_date,to_date)

@app.post("/audit_report")
def audit_report(Project_id:Annotated[int,Form()],Cycle_id:Annotated[int,Form()],from_date:Annotated[str,Form()],to_date:Annotated[str,Form()],db:Session=Depends(get_db)):
    df = crud.audit_report(db,Project_id,Cycle_id,from_date,to_date)
    csv_writer = StringIO()
    df.to_csv(csv_writer, index=False)
    csv_writer.seek(0)
    response = Response(content=csv_writer.getvalue(), media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=data.csv"
    response.headers["Content-Type"] = "text/csv"
    return response

@app.post("/audit_report_zip_file")
def audit_report_zip_file(Project_id:Annotated[int,Form()],Cycle_id:Annotated[int,Form()],from_date:Annotated[str,Form()],to_date:Annotated[str,Form()],db:Session=Depends(get_db)):
    return crud.audit_report_zip_file(db,Project_id,Cycle_id,from_date,to_date)

@app.post("/upc_report")
def upc_report(Project_id:Annotated[int,Form()],Cycle_id:Annotated[int,Form()],db:Session=Depends(get_db)):
    df = crud.upc_report(db,Project_id,Cycle_id)
    csv_writer = StringIO()
    df.to_csv(csv_writer, index=False)
    csv_writer.seek(0)
    response = Response(content=csv_writer.getvalue(), media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=data.csv"
    response.headers["Content-Type"] = "text/csv"
    return response

@app.post("/audit_upc_report")
def audit_upc_report(Project_id:Annotated[int,Form()],Cycle_id:Annotated[int,Form()],db:Session=Depends(get_db)):
    df = crud.audit_upc_report(db,Project_id,Cycle_id)
    csv_writer = StringIO()
    df.to_csv(csv_writer, index=False)
    csv_writer.seek(0)
    response = Response(content=csv_writer.getvalue(), media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=data.csv"
    response.headers["Content-Type"] = "text/csv"
    return response

@app.post("/pro_hourly_report")
def pro_hourly_report(Project_id:Annotated[int,Form()],Cycle_id:Annotated[int,Form()],picked_date:Annotated[str,Form()],db:Session=Depends(get_db)):
    df = crud.pro_hourly_report(db,picked_date,Project_id,Cycle_id)
    try:
        transformed_data = df.to_dict(orient="records")
        print(transformed_data)
        return transformed_data
    except:
        return ""
    
@app.post("/audit_hourly_report")
def audit_hourly_report(Project_id:Annotated[int,Form()],Cycle_id:Annotated[int,Form()],picked_date:Annotated[str,Form()],db:Session=Depends(get_db)):
    df = crud.audit_hourly_report(db,picked_date,Project_id,Cycle_id)
    try:
        transformed_data = df.to_dict(orient="records")
        print(transformed_data)
        return transformed_data
    except:
        return ""
    

@app.get("/finding_duplicates")
def finding_duplicates(db:Session=Depends(get_db)):
    return crud.finding_duplicates(db)

@app.get("/getting_duplicates")
def getting_duplicates(db:Session=Depends(get_db)):
    return crud.getting_duplicates(db)


@app.get("/get_list_of_project_cycles")
def get_list_of_project_cycles(db:Session=Depends(get_db)):
    return crud.get_list_of_project_cycles(db)


@app.post("/activate_cycle")
def activate_cycle(project_id:Annotated[int,Form()],cycle_id:Annotated[int,Form()],db:Session=Depends(get_db)):
    return crud.activate_cycle(db,project_id,cycle_id)

MEDIA_ROOT = '/home/a/Videos/Shell santhosh/backend/'

@app.post("/dup")
def add_duplicate_record(db: Session = Depends(get_db)):

    def get_highest_folder_name():
        upload_path = os.path.join(MEDIA_ROOT, 'upload_image')
        folders = [d for d in os.listdir(upload_path) if os.path.isdir(os.path.join(upload_path, d)) and d.isdigit()]
        if not folders:
            return None
        return str(max(map(int, folders)))
    
    def print_square(selected_folder):
        folder_path = os.path.join(MEDIA_ROOT, 'upload_image', selected_folder)
        media_root = os.path.join(MEDIA_ROOT, 'upload_image')
        compare_folder_images(folder_path, media_root, selected_folder)

    def compare_folder_images(folder_path, media_root, selected_folder):
        folder_data = load_folder_images(folder_path)
        duplicate_images = find_duplicate_images(folder_data, media_root, folder_path, selected_folder)
        return duplicate_images

    def load_folder_images(folder_path):
        folder_data = {}
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                path = os.path.join(root, file)
                with open(path, 'rb') as f:
                    folder_data[path] = f.read()
        return folder_data

    def find_duplicate_images(folder_data, directory, folder_path, selected_folder):
        duplicates = []
        for root, dirs, files in os.walk(directory):
            for file in files:
                path = os.path.join(root, file)

                for folder_filename, folder_file_content in folder_data.items():

                    relative_original = os.path.relpath(folder_filename, os.path.join(MEDIA_ROOT, 'upload_image'))
                    relative_duplicate = os.path.relpath(path, os.path.join(MEDIA_ROOT, 'upload_image'))

                    if relative_original != relative_duplicate and relative_original.endswith(('.jpg', '.png')) and relative_duplicate.endswith(('.jpg', '.png')):

                        split_relative_original = '/'.join(relative_original.split("/")[1:-1])
                        split_relative_duplicate = '/'.join(relative_duplicate.split("/")[1:-1])

                        four_d_code_org = os.path.basename(relative_original).split('.')[0][:4]
                        four_d_code_dups = os.path.basename(relative_duplicate).split('.')[0][:4]

                        if four_d_code_org == four_d_code_dups and split_relative_original == split_relative_duplicate:
                            if compare_images_opencv(folder_file_content, path):
                                if not is_hierarchy_folder(folder_path, path):
                                    duplicates.append((relative_original, relative_duplicate))
                                    
                                    new_record = models.duplicate_table(
                                        cycle_id=int(selected_folder),
                                        duplicate_cycle_id=int(relative_duplicate[0]),  # you can change this logic
                                        original_image_path='upload_image/' + relative_original,
                                        duplicate_image_path='upload_image/' + relative_duplicate
                                    )
                                    db.add(new_record)
                                    db.commit()
                                    db.refresh(new_record)
        return duplicates

    def compare_images_opencv(folder_image_content, existing_image_path):
        existing_image = cv2.imread(existing_image_path)
        if existing_image is None:
            return False
        try:
            folder_image = cv2.imdecode(np.frombuffer(folder_image_content, np.uint8), -1)
        except cv2.error:
            return False

        if folder_image is not None and folder_image.size != 0:
            if existing_image.shape == folder_image.shape:
                difference = cv2.subtract(existing_image, folder_image)
                b, g, r = cv2.split(difference)
                return cv2.countNonZero(b) == 0 and cv2.countNonZero(g) == 0 and cv2.countNonZero(r) == 0
        return False

    def is_hierarchy_folder(folder_path, path_to_check):
        return os.path.commonpath([folder_path]) == os.path.commonpath([folder_path, path_to_check])

    # Call function (example: folder '5')
    selected_folder = get_highest_folder_name()
    if selected_folder:
        print(f"Selected highest folder: {selected_folder}")
        print_square(selected_folder)
        duplicates = print_square(selected_folder)
        return duplicates

    return {"status": "success", "message": "Duplicate check completed and data saved."}


from typing import List

@app.get("/get_cycles", response_model=List[schemas.CycleInfo])
def get_cycles(db: Session = Depends(get_db)):
    db_res3 = db.query(models.shell_Tracking_Sheet).all()
    unique_cycles = {
        (item.cycle_id, item._cycle_table2.cycle_name)
        for item in db_res3 if item._cycle_table2
    }
    return [{"cycle_id": cid, "cycle_name": cname} for cid, cname in unique_cycles]



# @app.post("/submit_cycles")
# async def submit_cycles(selection: schemas.CycleSelection):
#     # Process the selected cycle IDs
#     print(f"Cycle 1 ID: {selection.cycle_1_id}")
#     print(f"Cycle 2 ID: {selection.cycle_2_id}")
 
#     return {"message": "Cycles received successfully!", "cycle_1_id": selection.cycle_1_id, "cycle_2_id": selection.cycle_2_id}

import hashlib
# --- Helper function to calculate SHA256 hash ---
def calculate_sha256(file_path, chunk_size=8192):
    """Calculates the SHA256 hash of a file efficiently."""
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            while chunk := f.read(chunk_size):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    except IOError as e:
        print(f"Error reading file {file_path}: {e}")
        return None # Handle error case

# --- Optimized Duplicate Check Function ---
def check_for_duplicates_optimized(cycle_1_id: int, cycle_2_id: int, db: Session):
    """
    Checks for duplicate images between two cycle folders using SHA256 hashing.
    Returns a list of tuples: [(relative_path_cycle1, relative_path_cycle2), ...]
    """
    db.query(models.duplicate_table).delete()
    db.commit()
    
    folder_1_path = os.path.join(MEDIA_ROOT, 'upload_image', str(cycle_1_id))
    folder_2_path = os.path.join(MEDIA_ROOT, 'upload_image', str(cycle_2_id))

    # Check if folders exist
    if not os.path.isdir(folder_1_path): # Use isdir for directories
        raise HTTPException(status_code=404, detail=f"Cycle folder not found: {folder_1_path}")
    if not os.path.isdir(folder_2_path):
        raise HTTPException(status_code=404, detail=f"Cycle folder not found: {folder_2_path}")

    print(f"Scanning folder 1: {folder_1_path}")
    hashes_folder1 = {}
    # --- Pass 1: Hash all images in the first folder ---
    for root, _, files in os.walk(folder_1_path):
        for filename in files:
            # Optional: Add basic file type check if needed (e.g., by extension)
            if not filename.lower().endswith(('.png', '.jpg', '.jpeg')):
                continue
            file_path = os.path.join(root, filename)
            file_hash = calculate_sha256(file_path)
            if file_hash:
                relative_path = os.path.relpath(file_path, folder_1_path)
                if file_hash not in hashes_folder1:
                    hashes_folder1[file_hash] = []
                hashes_folder1[file_hash].append(relative_path)

    print(f"Found {len(hashes_folder1)} unique image hashes in folder 1.")
    print(f"Scanning folder 2: {folder_2_path}")

    duplicates_found = []
    db_records_to_add = []

    # --- Pass 2: Hash images in the second folder and compare ---
    for root, _, files in os.walk(folder_2_path):
        for filename in files:
            # Optional: Add basic file type check if needed
            if not filename.lower().endswith(('.png', '.jpg', '.jpeg')):
                continue
            file_path = os.path.join(root, filename)
            file_hash = calculate_sha256(file_path)

            if file_hash and file_hash in hashes_folder1:
                # Found a match!
                relative_path_2 = os.path.relpath(file_path, folder_2_path)
                # This image in folder 2 is a duplicate of ALL images in folder 1 with the same hash
                for relative_path_1 in hashes_folder1[file_hash]:
                    print(f"Duplicate found: {relative_path_1} (Cycle {cycle_1_id}) and {relative_path_2} (Cycle {cycle_2_id})")
                    duplicates_found.append((relative_path_1, relative_path_2))

                    # Prepare DB record, but don't add/commit yet
                    new_record = models.duplicate_table(
                        cycle_id=cycle_1_id,
                        duplicate_cycle_id=cycle_2_id,
               
                        original_image_path=f'upload_image/{cycle_1_id}/{relative_path_1}',
                        duplicate_image_path=f'upload_image/{cycle_2_id}/{relative_path_2}'
                    )
                    db_records_to_add.append(new_record)



    # --- Batch Database Insert (Crucial for performance) ---
    if db_records_to_add:
        try:
            print(f"Adding {len(db_records_to_add)} duplicate records to the database.")
            db.add_all(db_records_to_add)
            db.commit()
            print("Database commit successful.")
        except Exception as e:
            print(f"Database error during bulk insert: {e}")
            db.rollback()
            raise HTTPException(status_code=500, detail=f"Failed to save duplicate records to database: {e}")

    return duplicates_found


# --- Update your FastAPI endpoint to use the optimized function ---
@app.post("/submit_cycles")
async def submit_cycles(selection: schemas.CycleSelection, db: Session = Depends(get_db)):
    cycle_1_id = selection.cycle_1_id
    cycle_2_id = selection.cycle_2_id

    if cycle_1_id == cycle_2_id:
         raise HTTPException(status_code=400, detail="Cannot compare a cycle with itself.")
    
    # Use the optimized function
    duplicates = check_for_duplicates_optimized(cycle_1_id, cycle_2_id, db)

    if duplicates:
        # Return only the paths, or structure as needed
        return {"message": f"Found {len(duplicates)} duplicate pairs.", "duplicates": duplicates}
    else:
        return {"message": "No duplicates found."}