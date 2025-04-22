# Shell

      git clone https://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@github.com/Elanchezhian2712/shell_apr22.git

------------------------

# Run Cmd
      Frontend : python3 -m http.server 8000 --bind 0.0.0.0
      Backend : bash run.sh

---------------------
      
# File Formate csv(Tracker File):
      Store Number
      4 - Digit Store Number	
      Store Name
      Department Name
      Planogram Type
      Planogram Name	
      No. of SKUs	Priroity
      Delivery_date
      
# Planogram PDF Formate Name:
      Planogram Name.zip

# Source File Name:
      4 digit Store number.zip

---------------------------------
# Query

    
      INSERT INTO "User_Roles" (role_id, role_name, role_status)
        VALUES
        (1, 'Admin', 1),
        (2, 'Audit', 1),
        (3, 'Production', 1),
        (6, 'Testing', 1);
    
    
    INSERT INTO "Shell_Menus" (menu_id, menu_name, menu_status)
    VALUES
    (1, 'Configrations', 1),
    (2, 'Production', 1),
    (3, 'Audit', 1),
    (4, 'Upload', 1),
    (5, 'Reports', 1),
    (6, 'Re_Allocation', 1),
    (7, 'Specialization', 1),
    (8, 'Duplicate', 1),
    (9, 'Active_Deactive', 1);
    
    
        INSERT INTO "Shell_User_table" (id, emp_id, name, mobile, working_location, designation, user_status, specialization)
    VALUES
    (1, 'MAH0001', 'Shan', '9874563210', 'Salem', 'trainee', 1, 'Ambient NAB,Chillers NAB');
        
        
        
            INSERT INTO "Shell_menu_permission" (id, role_id, menu_id)
    VALUES
    (2, 1, 1),
    (3, 1, 2),
    (5, 1, 4),
    (6, 1, 3),
    (7, 6, 3),
    (8, 3, 2),
    (9, 1, 5),
    (10, 1, 6),
    (11, 1, 7),
    (12, 1, 8),
    (13, 1, 9);
            
            
    INSERT INTO "Shell_AnnotateMarker" (
        annotatemarker_id,
        annotate_marker_shape_name,
        annotate_marker_shape_colour,
        annotate_marker_label_name,
        annotate_marker_status
    )
    VALUES
    (1, 'Tick', 'Red', 'Tick', 1);
    
    
    
    INSERT INTO "Shell_DataSource" (
        datasource_id,
        datasource_type,
        datasource_status
    )
    VALUES
    (1, 'PDF', 1);
    
    
    INSERT INTO "Shell_Demography" (
        demography_id,
        demography_name,
        demography_status
    )
    VALUES
    (1, 'poland', 1);
    
    
    INSERT INTO "Shell_Project_Creation_Table" (
        id,
        project_name,
        project_demography,
        project_input_type,
        project_status
    )
    VALUES
    (1, 'sample', 1, 1, 1);
    
    
    INSERT INTO "Shell_Project_Cycle_Creation" (
        id,
        project_id,
        cycle_name,
        cycle_start_date,
        cycle_end_date,
        cycle_status
    )
    VALUES
    (1, 1, 'cycle 1', '2025-04-19', '2026-08-14', 1),
    (2, 1, 'cycle 2', '2025-04-19', '2027-12-19', 1);
    
    
    
    INSERT INTO "Shell_Project_Selected_Annotation" (
        id,
        project_id,
        annotation_id,
        annotate_marker_status
    )
    VALUES
    (1, 1, 1, 1);
    
    INSERT INTO "Shell_Project_User_Role_table" (id, user_id, project_id, role_id, status) values (1, 'MAH0001', 1, 1, 1);    
