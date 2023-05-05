import os
import os.path
import shutil
import pathlib
import hashlib
import csv
import uuid
import time
from datetime import datetime
from pyrsistent import thaw
from zipfile import ZipFile
from os.path import basename
from pyPreservica import *
from cleanup_dates import *
from openpyxl import load_workbook
from deepdiff import DeepDiff

client = EntityAPI()

# Project Log File variables by index
# 0 - date_time
# 1 - container

#Windows Project Path
#proj_path = 'windows path to project folder'
#Linux Project Path
# proj_path = 'linux path to project folder'

proj_log_file = os.path.join(proj_path, 'project_log.txt')

def work_order_cleanup():
    filename = r'Excel project spreadsheet based on NYU plugin output after Lisa works with it'
    wb = load_workbook(filename)
    ws = wb['digitization_work_order_report']
    #review bounds of spreadsheet, espcially maxrow
    #columns may change depending on what columns deleted
    mincol = 10
    maxcol = 10
    minrow = 2
    maxrow = 151
    iterrow = 2
    cuidcol = 7
    filecol = 10
    datecol = 9
    # Miner = 'MED001'
    # RBSCP UA = 'RCL001'
    # Robbins = 'RCL002'
    # RBSCP HM = 'RCL003'
    # RBSCP BK = 'RCL004'
    orgid = 'RCL003'
    for row in ws.iter_rows(min_row = minrow, min_col = mincol, max_row = maxrow, max_col = maxcol):
        for cell in row:
            filevar = orgid + '_' + ws.cell(row = iterrow, column = cuidcol).value
            ws.cell(row = iterrow, column = filecol).value = filevar
            iterrow += 1
            if ws.cell(row = iterrow, column=datecol).value == None:
                ws.cell(row = iterrow, column=datecol).value = 'undated'
    wb.save(filename)
# work_order_cleanup()

#this function takes the folder containing all the preservation masters and renames to be the "container" folder which will ultimately be used for OPEX incremental ingest
#also creates a "project_log.txt" file to store variables so that an ingest project can be worked on over multiple sessions
def create_container():
    print('----CREATING CONTAINER----')
    project_log_hand = open(proj_log_file, 'a')
    now = datetime.now()
    date_time = now.strftime('%Y-%m-%d_%H-%M-%S')
    project_log_hand.write(date_time + '\n')
    container = 'container_' + date_time
    os.mkdir(os.path.join(proj_path, container))
    project_log_hand.write(container + '\n')
    temp_folder = os.path.join(proj_path, 'DPS001_2022-09_RFP_phase3')
    for file in os.listdir(path = temp_folder):
        if file.endswith('.tif'):
            shutil.move(os.path.join(proj_path, temp_folder, file), os.path.join(proj_path, container, file))
    print('Container directory: {} and moved digital assets into it'.format(container))
    project_log_hand.close()
# create_container()

#This function creates paths to the folders and files and then moves the files to their respective folders. 
def folder_ds_files():
    print('----CREATING FOLDER STRUCTURE FOR PRESERVATION MASTERS----')
    project_log_hand = open(proj_log_file, 'r')
    vars = project_log_hand.readlines()
    project_log_hand.close()
    container = vars[1].strip()
    folder_count = 0
    file_count = 0
    folder_list = list()
    path_container = os.path.join(proj_path, container)
    for file in os.listdir(path = path_container):
        file_root = file.split('-')[0].strip()
        if file_root not in folder_list:
            folder_list.append(file_root)
            print('added {} to folder_list'.format(file_root))
    for file_root in folder_list:
        path_folder = os.path.join(proj_path, container, file_root)
        os.mkdir(path_folder)
        print('created {}'.format(path_folder))
        folder_count += 1
    for file in os.listdir(path = path_container):
        if os.path.isdir(file) == True:
            continue
        else:
            path_file = os.path.join(proj_path, container, file)
            file_prefix = file.split('-')[0].strip()
            path_folder = os.path.join(proj_path, container, file_prefix, file)
            shutil.move(path_file, path_folder)
            print('moved {} to {}'.format(path_file, path_folder))
            file_count += 1
    print('created {} folders'.format(folder_count))
    print('moved {} files'.format(file_count))   
# folder_ds_files()            

#this function begins the process of creating the PAX structure necessary for ingest
#"Representation_Preservation" folder is created, and each image is given a separate subdir inside of it
def representation_preservation():
    print('----CREATING REPRESENTATION_PRESERVATION FOLDERS AND MOVING ASSETS INTO THEM----')
    project_log_hand = open(proj_log_file, 'r')
    vars = project_log_hand.readlines()
    project_log_hand.close()
    container = vars[1].strip()
    folder_count = 0
    file_count = 0
    path_container = os.path.join(proj_path, container)
    rep_pres = 'Representation_Preservation'
    for directory in os.listdir(path = path_container):
        path_directory = os.path.join(proj_path, container, directory)
        path = os.path.join(proj_path, container, directory, rep_pres)
        os.mkdir(path)
        folder_count += 1
        for file in os.listdir(path = path_directory):
            path_directoryfile = os.path.join(proj_path, container, directory, file)
            if file == rep_pres:
                continue
            else:
                file_name = file.split('.')[0]
                os.mkdir(os.path.join(path, file_name))
                print('created directory: {}'.format(path + '/' + file_name))
                shutil.move(path_directoryfile, os.path.join(path, file_name, file))
                print('moved file: {}'.format(path + '/' + file_name + '/' + file))
            file_count += 1
    print('Created {} Representation_Preservation directories | Moved {} files into created directories'.format(folder_count, file_count))
# representation_preservation()

#this function stages the "Representation_Preservation" folders for each asset inside a new directory
#this facilitates the creation of the zipped PAX package in the following function
def stage_pax_content():
    print('----STAGING PAX CONTENT IN PAX_STAGE----')
    project_log_hand = open(proj_log_file, 'r')
    vars = project_log_hand.readlines()
    container = vars[1].strip()
    project_log_hand.close()
    pax_count = 0
    rep_count = 0
    path_container = os.path.join(proj_path, container)
    for directory in os.listdir(path = path_container):
        path_directory = os.path.join(proj_path, container, directory)
        path_paxstage = os.path.join(proj_path, container, directory, 'pax_stage')
        os.mkdir(path_paxstage)
        pax_count += 1
        shutil.move(os.path.join(path_directory, 'Representation_Preservation'), path_paxstage)
        rep_count += 1
        print('created /pax_stage in {}'.format(directory))
    print('Created {} pax_stage subdirectories and staged {} representation subdirectories'.format(pax_count, rep_count))
# stage_pax_content()

#this function takes the contents of the "pax_stage" folder created in the previous function and writes them into a zip archive
#the zip archive is the PAX object that will eventually become an Asset in Preservica
def create_pax():
    print('----CREATING PAX ZIP ARCHIVES----')
    project_log_hand = open(proj_log_file, 'r')
    vars = project_log_hand.readlines()
    container = vars[1].strip()
    project_log_hand.close()
    dir_count = 0
    path_container = os.path.join(proj_path, container)
    for directory in os.listdir(path = path_container):
        path_zipdir = os.path.join(proj_path, container, directory, 'pax_stage/')
        path_directory = os.path.join(proj_path, container, directory)
        zip_dir = pathlib.Path(path_zipdir)
        pax_obj = ZipFile(os.path.join(path_directory, directory + '.zip'), 'w')
        for file_path in zip_dir.rglob("*"):
            pax_obj.write(file_path, arcname = file_path.relative_to(zip_dir))
        pax_obj.close()
        os.rename(os.path.join(path_directory, directory + '.zip'), os.path.join(path_directory, directory + '.pax.zip'))
        dir_count += 1
        print('created {}'.format(str(dir_count) + ': ' + directory + '.pax.zip'))
    print('Created {} PAX archives for ingest'.format(dir_count))
# create_pax()

#this function deletes the "pax_stage" folder and it's contents
def cleanup_directories():
    print('----REMOVING UNNECESSARY FILES----')
    project_log_hand = open(proj_log_file, 'r')
    vars = project_log_hand.readlines()
    project_log_hand.close()
    container = vars[1].strip()
    dir_count = 0
    project_log_hand = open(proj_log_file, 'a')
    path_container = os.path.join(proj_path, container)
    for directory in os.listdir(path = path_container):
        path_directory = os.path.join(proj_path, container, directory)
        for entity in os.listdir(path = path_directory):
            path_entity = os.path.join(proj_path, container, directory, entity)
            if entity == 'pax_stage':
                shutil.rmtree(path_entity)
                dir_count += 1
                print('removed pax_stage directory in {}'.format(directory))
    print('Deleted {} pax_stage folders'.format(dir_count))
    project_log_hand.close()
# cleanup_directories()

#this function creates the OPEX metadata file that accompanies an individual zipped PAX package
#this function also includes the metadata necessary for ArchivesSpace sync to Preservica
def pax_metadata():
    print('---CREATING METADATA FILES FOR PAX OBJECTS----')
    project_log_hand = open(proj_log_file, 'r')
    vars = project_log_hand.readlines()
    project_log_hand.close()
    container = vars[1].strip()
    dir_count = 0
    path_container = os.path.join(proj_path, container)
    for directory in os.listdir(path = path_container):
        path_directory = os.path.join(proj_path, container, directory)
        try:
            pax_hand = open(os.path.join(path_directory, directory + '.pax.zip'), 'rb')
            pax_read = pax_hand.read()
            sha1_checksum = hashlib.sha1(pax_read).hexdigest()
            pax_hand.close()
            id_file_hand = open('txt file containing ASpace ref|archival_object_number|title|date|cuid', 'r')
            id_file_lines = id_file_hand.readlines()
            id_file_hand.close()
            for line in id_file_lines:
                cuid = line.split('|')[4].strip()
                if cuid == directory:
                    ref_id = line.split('|')[0].strip()
                    title = line.split('|')[2].strip()
                    if '&' in title:
                        title = title.replace('&', 'and')
                    date_full = line.split('|')[3].strip()
                    date_formatted = aspace_dates(date_full)
                    display_title = '{title}{date_formatted}'.format(title=title, date_formatted=date_formatted)
                    opex = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.0">
    <opex:Transfer>
        <opex:Fixities>
            <opex:Fixity type="SHA-1" value="{sha1_checksum}"/>
        </opex:Fixities>
    </opex:Transfer>
    <opex:Properties>
        <opex:Title>{title}</opex:Title>
        <opex:Identifiers>
            <opex:Identifier type="code">{ref_id}</opex:Identifier>
        </opex:Identifiers>
    </opex:Properties>
    <opex:DescriptiveMetadata>
        <LegacyXIP xmlns="http://preservica.com/LegacyXIP">
            <AccessionRef>catalogue</AccessionRef>
        </LegacyXIP>
    </opex:DescriptiveMetadata>
</opex:OPEXMetadata>'''.format(sha1_checksum=sha1_checksum, title=display_title, ref_id=ref_id)
                    filename = directory + '.pax.zip.opex'
                    pax_md_hand = open(os.path.join(path_directory, filename), 'w')
                    pax_md_hand.write(opex)
                    pax_md_hand.close()
                    print('created {}'.format(filename))
                    dir_count += 1
        except:
            print('ERROR: {}'.format(directory))
            project_log_hand = open(proj_log_file, 'a')
            project_log_hand.write(directory + '\n')
            project_log_hand.close()
    print('Created {} OPEX metdata files for individual assets'.format(dir_count))
# pax_metadata()

#this function matches directory names (based on CUID) with archival object numbers from identifiers log file
#this metadata is another facet required for ArchivesSpace to Preservica synchronization
#NOTE if the title in the refid_aonum_cuid.txt file is surrounded by quotes, this function will fail
def ao_opex_metadata():
    print('----CREATE ARCHIVAL OBJECT OPEX METADATA----')
    project_log_hand = open(proj_log_file, 'r')
    vars = project_log_hand.readlines()
    container = vars[1].strip()
    project_log_hand.close()
    file_count = 0
    id_hand = open(os.path.join(proj_path, 'txt file containing ASpace ref|archival_object_number|title|date|cuid'), 'r')
    id_list = id_hand.readlines()
    id_hand.close()
    path_container = os.path.join(proj_path, container)
    for directory in os.listdir(path = path_container):
        path_directory = os.path.join(proj_path, container, directory)
        if directory.startswith('archival_object_'):
            continue
        else:
            for line in id_list:
                cuid = line.split('|')[4].strip()
                if cuid == directory:
                    ao_num_list = line.split('|')[1].strip()
                    ao_num = ao_num_list.split('/')[-1].strip()
                    ao_num_full = 'archival_object_' + ao_num
                    opex = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.0">
    <opex:Properties>
        <opex:Title>{ao_num}</opex:Title>
        <opex:Identifiers>
            <opex:Identifier type="code">{ao_num}</opex:Identifier>
        </opex:Identifiers>
    </opex:Properties>
    <opex:DescriptiveMetadata>
        <LegacyXIP xmlns="http://preservica.com/LegacyXIP">
            <Virtual>false</Virtual>
        </LegacyXIP>
    </opex:DescriptiveMetadata>
</opex:OPEXMetadata>'''.format(ao_num = ao_num_full)
                    ao_md_hand = open(os.path.join(path_directory, ao_num_full + '.opex'), 'w')
                    ao_md_hand.write(opex)
                    ao_md_hand.close()
                    os.rename(path_directory, os.path.join(proj_path, container, ao_num_full))
                    file_count += 1
        print('processed folder metadata in new foldername: {}'.format(ao_num_full))
    print('Created {} archival object metadata files'.format(file_count))
# ao_opex_metadata()

#this function creates the last OPEX metadata file required for the OPEX incremental ingest, for the container folder
#this OPEX file has the folder manifest to ensure that content is ingested properly
def write_opex_container_md():
    print('----CREATE CONTAINER OBJECT OPEX METADATA----')
    project_log_hand = open(proj_log_file, 'r')
    vars = project_log_hand.readlines()
    project_log_hand.close()
    container = vars[1].strip()
    opex1 = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.0">
    <opex:Transfer>
        <opex:Manifest>
            <opex:Folders>\n'''
    opex2 = ''
    path_container = os.path.join(proj_path, container)
    for directory in os.listdir(path = path_container):
        opex2 += '\t\t\t\t<opex:Folder>' + directory + '</opex:Folder>\n'
    opex3 = '''\t\t\t</opex:Folders>
        </opex:Manifest>
    </opex:Transfer>
</opex:OPEXMetadata>'''
    container_opex_hand = open(os.path.join(proj_path, container, container + '.opex'), 'w')
    container_opex_hand.write(opex1 + opex2 + opex3)
    print('Created OPEX metadata file for {} directory'.format(container))
    container_opex_hand.close()
# write_opex_container_md()

#this function moves the newly ingest assets from the "OPEX Ingest" folder
#to the "pending link" folder to prepare for ArchivesSpace synchronization
def move_opex_aspace():
    print('----MOVING ASSETS TO PENDING LINK----')
    opex_folder = client.descendants('db77b64a-64e8-4da2-9645-6f3fe92c3164')
    aspace_folder = client.folder('9370a695-6bd3-441c-8498-982538ee8718')
    count = 0
    for entity in opex_folder:
        client.move(entity, aspace_folder)
        count += 1
        print('moving item {}'.format(str(count)))
        time.sleep(2)
    print('moved {} entities'.format(str(count)))
# move_opex_aspace()

#this function moves the now empty archival_object_###### folders into a newly created container
#folder in "Pending Deletion" to make deletion of the empty folders easier
def move_aspace_trash():
    print('----MOVING EMPTY FOLDERS TO TRASH----')
    aspace_folder = client.descendants('9370a695-6bd3-441c-8498-982538ee8718')
    count = 0
    now = datetime.now()
    folder_title = now.strftime('%Y-%m-%d_%H-%M-%S') + '_Deletion'
    new_folder = client.create_folder(folder_title, "container folder to delete AO# folders", 'admin', '6564c3a1-36bf-4b09-ab00-a624ae303f06')
    dest_folder = client.folder(new_folder.reference)
    for entity in aspace_folder:
        test_var = entity.title
        if test_var.startswith('archival_object_'):
            client.move(entity, dest_folder)
            count += 1
            print('moving item {}'.format(str(count)))
            time.sleep(2)
    print('Moved {} folders into the trash'.format(str(count)))
# move_aspace_trash()

#this function uses the "code" identifier to pull the DPS identifier from the identifier
#pipe deliminited identifier list and adds it to the asset as an external identifier
def dps_identifier():
    print('----ADDING DPS IDENTIFIERS----')
    count = 0
    id_hand = open(os.path.join(proj_path, 'txt file containing ASpace ref|archival_object_number|title|date|cuid'), 'r')
    id_list = id_hand.readlines()
    id_hand.close()
    for line in id_list:
        code = line.split('|')[0].strip()
        for ident in filter(only_assets, client.identifier("code", code)):
            dps = line.split('|')[4].strip()
            asset = client.asset(ident.reference)
            client.add_identifier(asset, "dps", dps)
        print('adding {} to {}'.format(dps, ident.reference))
        count += 1
    print('added identifiers to {} digital assets'.format(str(count)))
# dps_identifier()

#this functions pulls all the asset refs so that they can be copy/pasted
#into the PREMIS generator CSV file
def ref_pull():
    print('----OUTPUTTING ALL THE ASSET REFS TO TERMINAL----')
    folder = client.folder("Preservica folder that new assets were ingested into")
    for asset in filter(only_assets, client.all_descendants(folder.reference)):
        print(asset.reference)
# ref_pull()

#this function generates PREMIS records for digital assets based on a CSV file
def premis_generator():
    print('----CREATING PREMIS RECORDS----')
    premis_folder = os.path.join(proj_path, 'premis')
    # os.mkdir(premis_folder)
    fhand = open('csv file that contains PREMIS information to go into template', 'r')
    csv_reader = csv.reader(fhand, delimiter=',')
    count = 0
    for row in csv_reader:
        count += 1
        preservica_uuid = row[0]
        rights_uuid = uuid.uuid4()
        rights_basis = row[1]
        rights_status = row[2]
        rights_jurisdiction = row[3]
        rights_date = row[4]
        rights_note = row[5]
        rights_doc_text = row[6]
        rights_doc_uri = row[7]
        event_1_uuid = uuid.uuid4()
        event_1_type = row[8]
        event_1_datetime = row[9]
        event_1_details = row[10]
        event_1_agent = row[11]
        premis = '''<premis:premis xmlns:premis="http://www.loc.gov/premis/v3" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/premis/v3 https://www.loc.gov/standards/premis/premis.xsd" version="3.0">
        <premis:object xsi:type="premis:intellectualEntity">
            <premis:objectIdentifier>
                <premis:objectIdentifierType>preservica_uuid</premis:objectIdentifierType>
                <premis:objectIdentifierValue>{preservica_uuid}</premis:objectIdentifierValue>
            </premis:objectIdentifier>
        </premis:object>
        <premis:rights>
            <premis:rightsStatement>
                <premis:rightsStatementIdentifier>
                    <premis:rightsStatementIdentifierType>rights_uuid</premis:rightsStatementIdentifierType>
                    <premis:rightsStatementIdentifierValue>{rights_uuid}</premis:rightsStatementIdentifierValue>
                </premis:rightsStatementIdentifier>
                <premis:rightsBasis authority="rightsBasis" authorityURI="http://id.loc.gov/vocabulary/preservation/rightsBasis" valueURI="http://id.loc.gov/vocabulary/preservation/rightsBasis/cop">{rights_basis}</premis:rightsBasis>
                <premis:copyrightInformation>
                    <premis:copyrightStatus>{rights_status}</premis:copyrightStatus>
                    <premis:copyrightJurisdiction>{rights_jurisdiction}</premis:copyrightJurisdiction>
                    <premis:copyrightStatusDeterminationDate>{rights_date}</premis:copyrightStatusDeterminationDate>
                    <premis:copyrightNote>{rights_note}</premis:copyrightNote>
                    <premis:copyrightDocumentationIdentifier>
                        <premis:copyrightDocumentationIdentifierType>{rights_doc_text}</premis:copyrightDocumentationIdentifierType>
                        <premis:copyrightDocumentationIdentifierValue>{rights_doc_uri}</premis:copyrightDocumentationIdentifierValue>
                    </premis:copyrightDocumentationIdentifier>
                </premis:copyrightInformation>
                <premis:linkingObjectIdentifier>
                    <premis:linkingObjectIdentifierType>preservica_uuid</premis:linkingObjectIdentifierType>
                    <premis:linkingObjectIdentifierValue>{preservica_uuid}</premis:linkingObjectIdentifierValue>
                </premis:linkingObjectIdentifier>
            </premis:rightsStatement>
        </premis:rights>
        <premis:event>
            <premis:eventIdentifier>
                <premis:eventIdentifierType>event_uuid</premis:eventIdentifierType>
                <premis:eventIdentifierValue>{event_1_uuid}</premis:eventIdentifierValue>
            </premis:eventIdentifier>
            <premis:eventType>{event_1_type}</premis:eventType>
            <premis:eventDateTime>{event_1_datetime}</premis:eventDateTime>
            <premis:eventDetailInformation>
                <premis:eventDetail>{event_1_details}</premis:eventDetail>
            </premis:eventDetailInformation>
            <premis:linkingAgentIdentifier>
                <premis:linkingAgentIdentifierType>local</premis:linkingAgentIdentifierType>
                <premis:linkingAgentIdentifierValue>{event_1_agent}</premis:linkingAgentIdentifierValue>
                <premis:linkingAgentRole authority="eventRelatedAgentRole" authorityURI="http://id.loc.gov/vocabulary/preservation/eventRelatedAgentRole" valueURI="http://id.loc.gov/vocabulary/preservation/eventRelatedAgentRole/imp">implementer</premis:linkingAgentRole>
            </premis:linkingAgentIdentifier>
            <premis:linkingObjectIdentifier>
                <premis:linkingObjectIdentifierType>preservica_uuid</premis:linkingObjectIdentifierType>
                <premis:linkingObjectIdentifierValue>{preservica_uuid}</premis:linkingObjectIdentifierValue>
            </premis:linkingObjectIdentifier>
        </premis:event>
    </premis:premis>'''.format(preservica_uuid=preservica_uuid, rights_uuid=rights_uuid, rights_basis=rights_basis, rights_status=rights_status, rights_jurisdiction=rights_jurisdiction, rights_date=rights_date, rights_note=rights_note, rights_doc_text=rights_doc_text, rights_doc_uri=rights_doc_uri, event_1_uuid=event_1_uuid, event_1_type=event_1_type, event_1_datetime=event_1_datetime, event_1_details=event_1_details, event_1_agent=event_1_agent)
        premis_path = os.path.join(proj_path, 'premis', preservica_uuid + '.xml')
        with open(premis_path, 'w') as premis_hand:
            premis_hand.write(premis)
        print('created {}'.format(premis_path))
    fhand.close()
    print('created {} PREMIS files'.format(count))
# premis_generator()

#this function attaches the PREMIS records created in the previous function
#to their corresponding digital assets in Preservica
#NOTE MAKE SURE THAT THE NUMBER OF LINES IN CSV MATCHES NUMBER OF ASSETS BEING INGESTED!!!!!!!!
def premis_attach():
    print('----ATTACHING PREMIS RECORDS TO ASSETS----')
    count = 0
    for file in os.listdir(path='premis'):
        pres_ref = file.split('.')[0].strip()
        asset = client.asset(pres_ref)
        file_path = os.path.join('premis', file)
        with open(file_path, 'r', encoding="utf-8") as md:
            asset = client.add_metadata(asset, "http://www.loc.gov/premis/v3", md)
            print('Appended PREMIS metadata to {}'.format(pres_ref))
        count += 1
    print('attached {} PREMIS files'.format(count))
# premis_attach()

#this function generates a dictionary of filename:hash values for both the Droid file manifest
#and the ingested Preservica assets (using the Preservica API) and then compares the two to
#ensure they are identical, and provides a report if that is not the case
def quality_control():
    print('---STARTING QA---')
    preservicadict = dict()
    droiddict = dict()
    root_folder = client.folder("Preservica folder that new assets were ingested into")
    asset_count = 0
    file_count = 0
    print('----MAKING DROID DICTIONARY---')
    with open('csv file output from Droid containing manifest of all files', newline = '') as csvfile:
        reader = csv.reader(csvfile, delimiter = ',', quotechar = '"')
        for row in reader: 
            if 'File' in row[8]:
                droiddict[row[4]] = row[12]
    print('---MAKING PRESERVICA DICTIONARY----')
    for asset in filter(only_assets, client.all_descendants(root_folder.reference)):
        # asset_count += 1
        for representation in client.representations(asset):
            for content_object in client.content_objects(representation):
                for generation in client.generations(content_object):
                    for bitstream in generation.bitstreams:
                        filename = bitstream.filename 
                        for algorithm,value in bitstream.fixity.items():
                            hash = value
                            preservicadict[filename] = hash    
    print('----COMPARING DICTIONARIES----')            
    diff = DeepDiff(preservicadict, droiddict, verbose_level=2)
    print(diff)
# quality_control()
