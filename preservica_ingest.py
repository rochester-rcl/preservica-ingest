import os
import re
import csv
import uuid
import string
import shutil
import hashlib
import img2pdf
from pathlib import Path
from datetime import datetime
from zipfile import ZipFile
from deepdiff import DeepDiff
import xml.etree.ElementTree as ET
from pyPreservica import *

#nonstandard packages:
# - img2pdf
# - deepdiff
# - pyPreservica

#-------------------------------------------------------------------------------------------------------------------------------------
# PROJECT PREPARATION
#-------------------------------------------------------------------------------------------------------------------------------------

proj_id = 'DPS###_YYYY-MM_SHORTNAME'
#the project path contains the code, documentation, and files that will be used to create PAX objects for ingest into Preservica 
proj_path = os.path.join('G:/', proj_id)

#this function creates a staging folder subdirectory within the project folder in which all the files being packaged are placed
def create_container():
    now = datetime.now()
    date_time = now.strftime('%Y-%m-%d_%H-%M-%S')
    container = 'container_' + date_time
    os.mkdir(os.path.join(proj_path, container))
    print(f'Container directory: {container}')
# create_container()

container = 'container_YYYY-MM-DD_HH-MM-SS'
path_container = os.path.join(proj_path, container)

#This function renames all of the files in a project's container folder based on the DPS naming conventions. 
def rename_files():
    for dirs in os.listdir(path_container):
        for subdirs in os.listdir(os.path.join(path_container, dirs)):
            xml_count = 0
            tiff_count = 0
            txt_count = 0
            for files in os.listdir(os.path.join(path_container, dirs, subdirs)):
                if files.endswith('.pdf'):
                    new_filename = subdirs + '.pdf'
                    os.rename(os.path.join(path_container, dirs, subdirs, files), os.path.join(path_container, dirs, subdirs, new_filename))
                    print('renamed {} to {}'.format(files, new_filename))
                elif files.endswith('.mets.xml'):
                    new_filename = subdirs + '.xml'
                    os.rename(os.path.join(path_container, dirs, subdirs, files), os.path.join(path_container, dirs, subdirs, new_filename))
                    print('renamed {} to {}'.format(files, new_filename))
                elif files.endswith('.xml'):
                    xml_count += 1
                    xml_num = str(xml_count).zfill(5)
                    new_filename = subdirs + '_' + xml_num + '.xml'
                    os.rename(os.path.join(path_container, dirs, subdirs, files), os.path.join(path_container, dirs, subdirs, new_filename))
                    print('renamed {} to {}'.format(files,new_filename))
                elif files.endswith('.tif'):
                    tiff_count += 1
                    tiff_num = str(tiff_count).zfill(5)
                    new_filename = subdirs + '_' + tiff_num + '.tif'
                    os.rename(os.path.join(path_container, dirs, subdirs, files), os.path.join(path_container, dirs, subdirs, new_filename))
                    print('renamed {} to {}'.format(files,new_filename))
                elif files.endswith('.txt'):
                    txt_count += 1
                    txt_num = str(txt_count).zfill(5)
                    new_filename = subdirs + '_' + txt_num + '.txt'
                    os.rename(os.path.join(path_container, dirs, subdirs, files), os.path.join(path_container, dirs, subdirs, new_filename))
                    print('renamed {} to {}'.format(files,new_filename))
# rename_files()

# this function creates uses the filenames to create a folder that will ultimately become an asset in Preservica, this is dependent
# on file naming conventions and assumes that all files associated with a given asset will have identical strings at the start of the file
def folder_datastreams():
    print('---SORTING ASSETS ASSET FOLDERS---')
    id_list = list()
    for file in os.listdir(path = path_container):
        file_id = file.split('.')[0]
        if '-' in id:
            file_id = file_id.split('-')[0]
        if file_id not in id_list:
            id_list.append(file_id)
    for id in id_list:
        os.mkdir(os.path.join(path_container, id))
    for file in os.listdir(path = path_container):
        if os.path.isdir(os.path.join(path_container, file)) == True:
            continue
        else:
            file_root = file.split('.')[0]
            if '-' in file_root:
                file_root = file_root.split('-')[0]
            shutil.move(os.path.join(path_container, file), os.path.join(path_container, file_root, file))
            print(f'moved: {file}')
# folder_datastreams()

#should you need to create PDFs for complex digital objects based on digital still images in Preservica, this is a very simple Python script to take care of that using the img2pdf library
def img_to_pdf():
    print('---CREATING PDFS FOR ASSETS---')
    for folder in os.listdir(path = path_container):
        imgs = list()
        for file in os.listdir(path = os.path.join(path_container, folder)):
            file_path = os.path.join(path_container, folder, file)
            if '.jpg' in file_path:
                imgs.append(file_path)
        with open(os.path.join(path_container, folder, folder + '.pdf'), 'wb') as pdf_convert:
            pdf_convert.write(img2pdf.convert(imgs))
        print(f'created PDF for {folder}')
# img_to_pdf()

#TODO RUN DROID REPORT
# we run a Droid report to provide a local file manifest and generate checksums at the point of origin - a quality assurance script
# later will compare the contents of the Droid report with what is in Preservica to ensure nothing was corrupted in transit

#we use Dublin Core Qualified/Dublin Core Terms for all item level metadata for assets and that first gets generated in spreadsheets,
#this script first loops through each asset directory and matches it up to an identifier column in the spreadsheet, then converts the
#tabular data into XML and drops the file in the folder
def dcq_md():
    print('---CONVERTING METADATA SPREADSHEET TO XML---')
    for asset_dir in os.listdir(path = path_container):
        asset_path = os.path.join(path_container, asset_dir)
        with open(os.path.join(proj_path, proj_id + '_DCQ.csv'), 'r', newline='', encoding='utf8') as dcq_md:
            csv_reader = csv.reader(dcq_md, delimiter=',', quotechar='"')
            headers = next(csv_reader)
            for record in csv_reader:
                identifier = record[0]
                if asset_dir == identifier:
                    xml_record = '<dcterms:dcterms xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" targetNamespace="http://purl.org/dc/terms/" xmlns="http://purl.org/dc/terms/" elementFormDefault="qualified" attributeFormDefault="unqualified">\n'
                    pos = 0
                    for field in record:
                        values = field.split('|')
                        for value in values:
                            xml_str = '\t<' + headers[pos] + '>' + value.strip() + '</' + headers[pos].split()[0] + '>\n'
                            if xml_str == '\t<' + headers[pos] + '></' + headers[pos].split()[0] + '>\n':
                                continue
                            if xml_str == '\t<' + headers[pos] + '>' + string.whitespace + '</' + headers[pos].split()[0] + '>\n':
                                continue
                            else:
                                xml_record += xml_str
                        pos += 1
                    xml_record += '</dcterms:dcterms>'
                    xml_hand = open(os.path.join(asset_path, asset_dir + '_MD.xml'), 'w', encoding='utf8')
                    xml_hand.write(xml_record)
                    xml_hand.close()
                    print(f'created metadata for {identifier}')
# dcq_md()

#This function renames and folders .srt caption files in order to conform with Preservica's file requirements for rendering captions. 
def folder_captions():
    print('---foldering captions---')
    for folder in os.listdir(path_container):
        os.mkdir(os.path.join(path_container, folder, 'English'))
        for files in os.listdir(os.path.join(path_container, folder)):
            if files.endswith('.srt'):
                os.rename(os.path.join(path_container, folder, files), os.path.join(path_container, folder, 'Video.srt'))
                shutil.move(os.path.join(path_container, folder, 'Video.srt'), os.path.join(path_container, folder, 'English'))
# folder_captions()

# This function should ONLY be used if the assets being ingested include .mets.xml files as metadata files in order to distinguish them from other
# .xml files that are part of the ingested asset. This can also be modified to strip out any other unneeded elements from the Droid file
# This function strips out the metadata file lines from the Droid report as they are not needed and won't be ingested as files into Preservica

def cleanup_droid_metsxml():
    print('----CLEANING UP DROID FILE----')
    droid_hand_read = open(proj_id + '_droid.csv', 'r', encoding='utf8', newline='')
    csv_reader = csv.reader(droid_hand_read, delimiter=',', quotechar='"')
    new_droid = list()
    for line in csv_reader:
        if '.mets.xml' in str(line):
            continue
        else:
            new_droid.append(line)
    droid_hand_read.close()
    droid_hand_read = open(proj_id + '_droid.csv', 'w', encoding='utf8', newline='')
    csv_writer = csv.writer(droid_hand_read, delimiter=',', quotechar='"')
    csv_writer.writerows(new_droid)
    droid_hand_read.close()
    print('Droid file cleaned up!')
# cleanup_droid_metsxml()

#-------------------------------------------------------------------------------------------------------------------------------------
# CREATING THE PAX OBJECT
#-------------------------------------------------------------------------------------------------------------------------------------

# this function begins the process of creating the PAX structure necessary for ingest
# "Representation_Preservation"  and "Representation_Access" folders are created, 
# and each image is given a separate subdir inside of it
def representation_preservation_access():
    print('---SORTING ASSETS INTO REPRESENTATION FOLDERS---')
    count = 0
    for directory in os.listdir(path = path_container):
        path_directory = os.path.join(path_container, directory)
        count += 1
        rep_acc = 'Representation_Access'
        path_repacc = os.path.join(path_directory, rep_acc)
        os.mkdir(path_repacc)
        rep_pres = 'Representation_Preservation'
        path_reppres = os.path.join(path_directory, rep_pres)
        os.mkdir(path_reppres)
        tiff_count = 0
        for file in os.listdir(path = path_directory):
            path_file = os.path.join(path_directory, file)
            file_name = file.split('.')[0]
            if file.endswith('.tif') or file.endswith('.tiff'):
                tiff_count += 1
                os.mkdir(os.path.join(path_reppres, file_name))
                shutil.move(path_file, os.path.join(path_reppres, file_name, file))
            if file.endswith('.pdf'):
                os.mkdir(os.path.join(path_repacc, file_name))
                shutil.move(path_file, os.path.join(path_repacc, file_name, file))
        print(f'{count} - {directory}')
    print(f'created Representation folders in {count} directories')
# representation_preservation_access()

#this function stages the "Representation_Access" and "Representation_Preservation" folders for each asset inside a new directory
#this facilitates the creation of the zipped PAX package in the following function
def stage_pax_content():
    print('----STAGING PAX CONTENT IN PAX_STAGE----')
    pax_count = 0
    rep_count = 0
    path_container = os.path.join(proj_path, container)
    for directory in os.listdir(path = path_container):
        path_directory = os.path.join(proj_path, container, directory)
        path_paxstage = os.path.join(proj_path, container, directory, 'pax_stage')
        os.mkdir(path_paxstage)
        pax_count += 1
        shutil.move(os.path.join(path_directory, 'Representation_Access'), path_paxstage)
        shutil.move(os.path.join(path_directory, 'Representation_Preservation'), path_paxstage)
        rep_count += 2
        print(f'{pax_count}: created /pax_stage in {directory}')
    print(f'Created {pax_count} pax_stage subdirectories and staged {rep_count} representation subdirectories')
# stage_pax_content()

#these two functions represent a variant of the above two functions
#this function begins the process of creating the PAX structure necessary for ingest
#"Representation_Preservation" folder is created, and each image is given a separate subdir inside of it
def representation_preservation_access():
    print('---SORTING ASSETS INTO REPRESENTATION FOLDERS---')
    count = 0
    for directory in os.listdir(path = path_container):
        path_directory = os.path.join(proj_path, container, directory)
        ascii_doc = directory + '_transcript.asc'
        file_list = os.listdir(path = path_directory)
        if ascii_doc in file_list:
            count += 1
            rep_acc_1 = 'Representation_Access_1'
            rep_acc_2 = 'Representation_Access_2'
            path_repacc_1 = os.path.join(proj_path, container, directory, rep_acc_1)
            path_repacc_2 = os.path.join(proj_path, container, directory, rep_acc_2)
            os.mkdir(path_repacc_1)
            os.mkdir(path_repacc_2)
            for file in os.listdir(path = path_directory):
                path_file = os.path.join(proj_path, container, directory, file)
                if file in (rep_acc_1, rep_acc_2):
                    continue
                elif file.endswith('.xml'):
                    continue
                elif file.endswith('.asc'):
                    os.mkdir(os.path.join(path_repacc_1, directory + '_transcript'))
                    shutil.move(path_file, os.path.join(path_repacc_1, directory + '_transcript', file))
                elif file.endswith('.pdf'):
                    os.mkdir(os.path.join(path_repacc_2, directory + '_document'))
                    shutil.move(path_file, os.path.join(path_repacc_2, directory + '_document', file))
            print(f'{count}: Representation Access 1 & 2')
        else:
            count += 1
            rep_acc = 'Representation_Access'
            path_repacc = os.path.join(proj_path, container, directory, rep_acc)
            os.mkdir(path_repacc)
            for file in os.listdir(path = path_directory):
                path_file = os.path.join(proj_path, container, directory, file)
                if file in (rep_acc):
                    continue
                elif file.endswith('.xml'):
                    continue
                elif file.endswith('.pdf'):
                    os.mkdir(os.path.join(path_repacc, directory + '_document'))
                    shutil.move(path_file, os.path.join(path_repacc, directory + '_document', file))
            print(f'{count}: Representation Acess')
    print(f'created Representation folders in {count} directories')
# representation_preservation_access()

#this function stages the "Representation_Access" and "Representation_Preservation" folders for each asset inside a new directory
#this facilitates the creation of the zipped PAX package in the following function
def stage_pax_content():
    print('----STAGING PAX CONTENT IN PAX_STAGE----')
    pax_count = 0
    rep_count = 0
    path_container = os.path.join(proj_path, container)
    for directory in os.listdir(path = path_container):
        path_directory = os.path.join(proj_path, container, directory)
        folder_list = os.listdir(path = path_directory)
        path_paxstage = os.path.join(proj_path, container, directory, 'pax_stage')
        os.mkdir(path_paxstage)
        if 'Representation_Access_1' in folder_list:
            pax_count += 1
            shutil.move(os.path.join(path_directory, 'Representation_Access_1'), path_paxstage)
            shutil.move(os.path.join(path_directory, 'Representation_Access_2'), path_paxstage)
            rep_count += 2
            print(f'{pax_count}: created /pax_stage in {directory}')
        else:
            pax_count += 1
            shutil.move(os.path.join(path_directory, 'Representation_Access'), path_paxstage)
            rep_count += 1
            print(f'{pax_count}: created /pax_stage in {directory}')
    print(f'Created {pax_count} pax_stage subdirectories and staged {rep_count} representation subdirectories')
# stage_pax_content()

#this is a variant of the the stage_pax_content() function which uses Pathlib and rglob in order to grab all of the
#Access and Preservation Represenation folders, without having to know the data model for the given PAX object in advance
def stage_pax_content_pathlib():
    print('----STAGING PAX CONTENT IN PAX_STAGE----')
    pax_count = 0
    rep_count = 0
    path_container = os.path.join(proj_path, container)
    for directory in os.listdir(path = path_container):
        path_directory = os.path.join(proj_path, container, directory) 
        pathlib_dir = Path(path_directory)
        rep_dirs = pathlib_dir.glob('Representation_*')
        path_paxstage = os.path.join(proj_path, container, directory, 'pax_stage')
        os.mkdir(path_paxstage)
        pax_count += 1
        for rep_dir in rep_dirs:
            shutil.move(rep_dir, path_paxstage)
            rep_count += 1
        print(f'{pax_count}: created /pax_stage in {directory}')
    print(f'Created {pax_count} pax_stage subdirectories and staged {rep_count} representation subdirectories')
# stage_pax_content_pathlib()

#this function takes the contents of the "pax_stage" folder created in the previous function and writes them into a zip archive
#the zip archive is the PAX object that will eventually become an Asset in Preservica
def create_pax():
    print('----CREATING PAX ZIP ARCHIVES----')
    dir_count = 0
    path_container = os.path.join(proj_path, container)
    for directory in os.listdir(path = path_container):
        path_zipdir = os.path.join(proj_path, container, directory, 'pax_stage/')
        path_directory = os.path.join(proj_path, container, directory)
        zip_dir = Path(path_zipdir)
        pax_obj = ZipFile(os.path.join(path_directory, directory + '.pax.zip'), 'w')
        for file_path in zip_dir.rglob("*"):
            pax_obj.write(file_path, arcname = file_path.relative_to(zip_dir))
        pax_obj.close()
        dir_count += 1
        print(f'{dir_count}: created {directory}.pax.zip')
    print(f'Created {dir_count} PAX archives for ingest')
# create_pax()

#-------------------------------------------------------------------------------------------------------------------------------------
# CREATING THE OPEX METADATA
#-------------------------------------------------------------------------------------------------------------------------------------

#this function creates the OPEX metadata file that accompanies an individual zipped PAX package
#this function also includes the metadata necessary for ArchivesSpace sync to Preservica
def pax_metadata():
    print('---CREATING METADATA FILES FOR PAX OBJECTS----')
    metadata_count = 0
    for folder in os.listdir(path = path_container):
        metadata_count += 1
        path_folder = os.path.join(path_container, folder)
        desc_md = ''
        title = ''
        sha1_checksum = ''
        for file in os.listdir(path = path_folder):
            path_file = os.path.join(path_folder, file)
            if file.endswith('.zip'):
                pax_hand = open(path_file, 'rb')
                pax_read = pax_hand.read()
                sha1_checksum = hashlib.sha1(pax_read).hexdigest()
                pax_hand.close()
            elif file.endswith('_MD.xml'):
                dmd_hand = open(path_file, 'r', encoding='utf8')
                dmd = dmd_hand.read().strip()
                title = re.findall('<dc:title>(.+?)</dc:title>', dmd)[0]
                desc_md += dmd + '\n'
                dmd_hand.close()
            opex = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.0">
        <opex:Properties>
            <opex:Title>{title}</opex:Title>
            <opex:Identifiers>
                <opex:Identifier type="dps">{folder}</opex:Identifier>
                <opex:Identifier type="project_id">{proj_id}</opex:Identifier>
            </opex:Identifiers>
            <SecurityDescriptor>public</SecurityDescriptor> 
        </opex:Properties>
        <opex:Transfer>
            <opex:Fixities>
                <opex:Fixity type="SHA-1" value="{sha1_checksum}"/>
            </opex:Fixities>
        </opex:Transfer>
        <opex:DescriptiveMetadata>
            {desc_md}
        </opex:DescriptiveMetadata>
    </opex:OPEXMetadata>'''
            opex = opex.replace(' & ', ' and ')
            pax_md_hand = open(os.path.join(path_folder, folder + '.pax.zip.opex'), 'w', encoding='utf8')
            pax_md_hand.write(opex)
            pax_md_hand.close()
        print(f'{metadata_count}: created {folder}.pax.zip.opex')
    print(f'Created {metadata_count} OPEX metdata files for individual assets')
# pax_metadata()

#this function loops through ever directory in "container" and opens up the OPEX metadata for the asset storing the entire contents in a string variable
#then the function loops through a text file that was manually created, containing the call number identifier as well as the ArchivesSpace archival object number
#while looping through the text file, if a match is found the OPEX metadata string variable, a metadata file is created for the folder
#this metadata is another facet required for ArchivesSpace to Preservica synchronization
def ao_opex_metadata():
    print('----CREATE ARCHIVAL OBJECT OPEX METADATA----')
    file_count = 0
    for directory in os.listdir(path = path_container):
        path_directory = os.path.join(path_container, directory)
        opex = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.0">
<opex:Properties>
    <opex:Title>{directory}</opex:Title>
    <opex:Identifiers>
        <opex:Identifier type="code">{directory}</opex:Identifier>
    </opex:Identifiers>
</opex:Properties>
<opex:DescriptiveMetadata>
    <LegacyXIP xmlns="http://preservica.com/LegacyXIP">
        <Virtual>false</Virtual>
    </LegacyXIP>
</opex:DescriptiveMetadata>
</opex:OPEXMetadata>'''
        ao_md_hand = open(os.path.join(path_directory, directory + '.opex'), 'w')
        ao_md_hand.write(opex)
        ao_md_hand.close()
        file_count += 1
        print(f'({file_count}) created metadata for {directory}')
    print(f'Created {file_count} archival object metadata files')
# ao_opex_metadata()

# this function is for Preservica ingests of born digital files where the file is the entire Preservica asset
# this function will look for evidence that ArchivesSpace sync is happening and adjust to create metadata to
# conform with that need.
def born_digital_opex():
    proj_path = Path(path_container)
    proj_files = proj_path.rglob('*')
    for entity in proj_files:
        if entity.suffix == '.opex':
            continue
        elif 'archival_object_' in entity.name:
            opex = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.0">
        <opex:Properties>
            <opex:Title>{entity.name}</opex:Title>
            <opex:Identifiers>
                <opex:Identifier type="code">{entity.name}</opex:Identifier>
            </opex:Identifiers>
        </opex:Properties>
        <opex:DescriptiveMetadata>
            <LegacyXIP xmlns="http://preservica.com/LegacyXIP">
                <Virtual>false</Virtual>
            </LegacyXIP>
        </opex:DescriptiveMetadata>
    </opex:OPEXMetadata>'''
            with open(entity.joinpath(entity.name + '.opex'), 'w', encoding='utf8') as ao_opex:
                ao_opex.write(opex)
            print('created: ', entity.name + '.opex')
        elif entity.is_file():
            fhand = open(entity, 'rb')
            fread = fhand.read()
            checksum = hashlib.sha1(fread).hexdigest()
            fhand.close()
            opex = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.0">
        <opex:Properties>
            <opex:Title>{entity.stem}</opex:Title>
            <SecurityDescriptor>public</SecurityDescriptor> 
        </opex:Properties>
        <opex:Transfer>
            <opex:Fixities>
                <opex:Fixity type="SHA-1" value="{checksum}"/>
            </opex:Fixities>
        </opex:Transfer>
    </opex:OPEXMetadata>'''
            with open(entity.parent.joinpath(entity.name + '.opex'), 'w', encoding='utf8') as file_opex:
                file_opex.write(opex)
            print('created: ', entity.name + '.opex')
        elif entity.is_dir():
            code = uuid.uuid4()
            file_manifest = ''
            desc_md = ''
            if 'archival_object_' in entity.parent.name:
                desc_md = '\n\t<opex:DescriptiveMetadata>\n\t\t<LegacyXIP xmlns="http://preservica.com/LegacyXIP">\n\t\t\t<AccessionRef>catalogue</AccessionRef>\n\t\t</LegacyXIP>\n\t</opex:DescriptiveMetadata>\n'
            for file in entity.glob('*'):
                file_size = file.stat().st_size
                file_name = file.name
                if file.suffix == '.opex':
                    continue
                else:
                    file_manifest += f'\t\t\t<opex:File type="content" size="{file_size}">{file_name}</opex:File>\n'
            opex = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.0">
        <opex:Properties>
            <opex:Title>{entity.name}</opex:Title>
            <opex:Identifiers>
                <opex:Identifier type="code">{code}</opex:Identifier>
            </opex:Identifiers>
            <SecurityDescriptor>public</SecurityDescriptor> 
        </opex:Properties>
        <opex:Transfer>
            <opex:Files>
    {file_manifest}\t\t</opex:Files>
        </opex:Transfer>{desc_md}
    </opex:OPEXMetadata>'''
            with open(entity.joinpath(entity.name + '.opex'), 'w', encoding='utf8') as folder_opex:
                folder_opex.write(opex)
            print('created: ', entity.name + '.opex')
# born_digital_opex()

#-------------------------------------------------------------------------------------------------------------------------------------
# PREPARING FOR INGEST
#-------------------------------------------------------------------------------------------------------------------------------------

#this function deletes many files and folders that have now served their purpose in the migration process
#all metadata files are deleted as well as the "pax_stage" folder and it's contents
#a warning is thrown up and directory and file name information written to "project_log.txt" if an unexpected file is discovered
def cleanup_directories():
    print('----REMOVING UNNECESSARY FILES----')
    file_count = 0
    dir_count = 0
    unexpected = 0
    path_container = os.path.join(proj_path, container)
    for directory in os.listdir(path = path_container):
        path_directory = os.path.join(proj_path, container, directory)
        for entity in os.listdir(path = path_directory):
            path_entity = os.path.join(proj_path, container, directory, entity)
            if entity.endswith('.zip') == True:
                print('PAX: ' + entity)
            elif entity.endswith('.opex') == True:
                print('metadata: ' + entity)
            elif entity.endswith('.xml') == True:
                os.remove(path_entity)
                file_count += 1
                print('removed metadata file')
            elif entity == 'pax_stage':
                shutil.rmtree(path_entity)
                dir_count += 1
                print('removed pax_stage directory')
            else:
                print('***UNEXPECTED ENTITY: ' + entity)
                unexpected += 1
    print(f'Deleted {file_count} metadata files and {dir_count} Representation_Preservation and Representation_Access folders')
    print(f'Found {unexpected} unexpected entities')
# cleanup_directories()

#this script will take assets out of individual folders where they were constructed and stage them directly
#in the container folder where they can be ingested en masse
def restage_content():
    print('---RESTAGING CONTENT---')
    count = 0
    for directory in os.listdir(path=path_container):
        directory_path = os.path.join(path_container, directory)
        for file in os.listdir(path=directory_path):
            file_path = os.path.join(path_container, directory, file)
            shutil.move(file_path, os.path.join(path_container, file))
        os.rmdir(directory_path)
        count += 1
        print(f'{count}: {directory_path}')
    print(f'restaed {count} directories')
# restage_content()

#TODO transfer container folder to Preservica bulk bucket

#-------------------------------------------------------------------------------------------------------------------------------------
# POST INGEST ACTIVITIES
#-------------------------------------------------------------------------------------------------------------------------------------

#creates a one-column CSV file with all of the Preservica Ref IDs for assets ingested into the system
#this function defaults to the Ref ID for the OPEX ingest folder and thus should not need to be updated
#but needs to be run before transfering assets out of the OPEX ingest folder
def ref_pull():
    print('----CREATING REF ID CSV----')
    count = 0
    ref_csv = open(proj_id + '_refs.csv', 'w', newline='', encoding='utf8')
    ref_writer = csv.writer(ref_csv, delimiter=',', quotechar='"')
    client = EntityAPI()
    opex_folder = client.descendants('db77b64a-64e8-4da2-9645-6f3fe92c3164')
    for entity in opex_folder:
        ref_writer.writerow([entity.reference])
        count += 1
        print(f'{count}: pulled {entity.reference}')
    ref_csv.close()
    print(f'created {proj_id}_refs.csv')
# ref_pull()

#this function generates PREMIS metadata for details about the assets not captured by system, such as creation date
#for digitized items, copyright status of materials, and extra details about the ingestion and/or migration of materials
#and then appends the metadata to the asset within Preservica. This uses the Ref IDs stored in the CSV file generated by
#the ref_pull() function
def premis_generator():
    print('----CREATING PREMIS RECORDS----')
    client = EntityAPI()
    fhand = open(proj_id + '_refs.csv', 'r', newline='')
    csv_reader = csv.reader(fhand, delimiter=',', quotechar='"')
    count = 0
    for row in csv_reader:
        count += 1
        preservica_uuid = row[0]
        rights_uuid = uuid.uuid4()
        rights_basis = 'copyright' #or license, statute, institutional policy, other
        rights_status = 'copyrighted' #copyrighted, unknown, public domain
        rights_jurisdiction = 'us'
        rights_date = '2024-06-29'
        rights_note = 'Published by the Friends of Mount Hope Cemetery and protected by copyright, which is held by the the organization.'
        rights_doc_text = 'In Copyright'
        rights_doc_uri = 'http://rightsstatements.org/vocab/InC/1.0/'
        event_1_uuid = uuid.uuid4()
        event_1_type = 'migration'
        event_1_datetime = '2024-10-02'
        event_1_details = 'Migrated as part of larger project to move from Islandora 7 repositories to Preservica over the course of 2024.'
        event_1_agent = 'John Dewees, Senior Digital Asset Management Specialist, Digital Initiatives department, River Campus Libraries, University of Rochester'
        event_2_uuid = uuid.uuid4()
        event_2_type = 'creation'
        event_2_datetime = '2020-2023'
        event_2_details = 'reformatted digital'
        event_2_agent = 'Rare Books, Special Collections, and Preservation department, River Campus Libraries, University of Rochester'
        premis = f'''<premis:premis xmlns:premis="http://www.loc.gov/premis/v3" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/premis/v3 https://www.loc.gov/standards/premis/premis.xsd" version="3.0">
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
        <premis:event>
            <premis:eventIdentifier>
                <premis:eventIdentifierType>event_uuid</premis:eventIdentifierType>
                <premis:eventIdentifierValue>{event_2_uuid}</premis:eventIdentifierValue>
            </premis:eventIdentifier>
            <premis:eventType>{event_2_type}</premis:eventType>
            <premis:eventDateTime>{event_2_datetime}</premis:eventDateTime>
            <premis:eventDetailInformation>
                <premis:eventDetail>{event_2_details}</premis:eventDetail>
            </premis:eventDetailInformation>
            <premis:linkingAgentIdentifier>
                <premis:linkingAgentIdentifierType>local</premis:linkingAgentIdentifierType>
                <premis:linkingAgentIdentifierValue>{event_2_agent}</premis:linkingAgentIdentifierValue>
                <premis:linkingAgentRole authority="eventRelatedAgentRole" authorityURI="http://id.loc.gov/vocabulary/preservation/eventRelatedAgentRole" valueURI="http://id.loc.gov/vocabulary/preservation/eventRelatedAgentRole/imp">implementer</premis:linkingAgentRole>
            </premis:linkingAgentIdentifier>
            <premis:linkingObjectIdentifier>
                <premis:linkingObjectIdentifierType>preservica_uuid</premis:linkingObjectIdentifierType>
                <premis:linkingObjectIdentifierValue>{preservica_uuid}</premis:linkingObjectIdentifierValue>
            </premis:linkingObjectIdentifier>
        </premis:event>
    </premis:premis>'''
        asset = client.asset(preservica_uuid)
        client.add_metadata(asset, "http://www.loc.gov/premis/v3", premis)
        print(f'{count}: Appended PREMIS metadata to {preservica_uuid}')
    fhand.close()
    print(f'appended {count} PREMIS files')
# premis_generator()

# #this function generates a dictionary of filename:hash values for both the Droid file manifest
# #and the ingested Preservica assets (using the Preservica API) and then compares the two to
# #ensure they are identical, and provides a report if that is not the case
#TODO update Droid CSV file name
def quality_control():
    print('---STARTING QA---')
    asset_count = 0
    print('----MAKING DROID DICTIONARY---')
    droiddict = dict()
    with open(proj_id + '_droid.csv', newline = '') as csvfile:
        reader = csv.reader(csvfile, delimiter = ',', quotechar = '"')
        for row in reader: 
            if 'File' in row[8]:
                droiddict[row[4]] = row[12]
    print('---MAKING PRESERVICA DICTIONARY----')
    client = EntityAPI()
    preservicalist = list()
    fhand = open(proj_id + '_refs.csv', 'r')
    csv_reader = csv.reader(fhand, delimiter=',')
    for line in csv_reader:
        preservicalist.append(line[0])
    fhand.close()
    preservicadict = dict()
    for reference in preservicalist:
        asset = client.asset(reference)
        asset_count += 1
        print(f'count: {asset_count} / {len(preservicalist)}')
        for representation in client.representations(asset):
            for content_object in client.content_objects(representation):
                for generation in client.generations(content_object):
                    for bitstream in generation.bitstreams:
                        filename = bitstream.filename 
                        for algorithm,value in bitstream.fixity.items():
                            preservicadict[filename] = value    
    print('----COMPARING DICTIONARIES----')            
    diff = DeepDiff(preservicadict, droiddict, verbose_level=2)
    if len(diff) == 0:
        print('QUALITY ASSURANCE PASSED')
    else:
        print(diff)
    # only used for troubleshoooting if comparing dictionaries fails
    # print('----DROID DICTIONARY----')
    # print(droiddict)
    # print('----PRESERVICA DICTIONARY----')
    # print(preservicadict)
# quality_control()

# this function generates a dictionary of filename:hash values for both the Droid file manifest and the ingested
# Preservica assets (using the Preservica API) and then compares the two to ensure they are identical, and creates
# CSV files of each dictionary if quality control fails so that they can be further compared
def quality_control_csv():
    print('---STARTING QA---')
    asset_count = 0
    print('----MAKING DROID DICTIONARY---')
    droiddict = dict()
    with open(proj_id + '_droid.csv', newline = '') as csvfile:
        reader = csv.reader(csvfile, delimiter = ',', quotechar = '"')
        for row in reader: 
            if 'File' in row[8]:
                droiddict[row[4]] = row[12]
    print('---MAKING PRESERVICA DICTIONARY----')
    client = EntityAPI()
    preservicalist = list()
    fhand = open(proj_id + '_refs.csv', 'r')
    csv_reader = csv.reader(fhand, delimiter=',')
    for line in csv_reader:
        preservicalist.append(line[0])
    fhand.close()
    preservicadict = dict()
    for reference in preservicalist:
        asset = client.asset(reference)
        asset_count += 1
        print('count: {} / {}'.format(asset_count, len(preservicalist)))
        for representation in client.representations(asset):
            for content_object in client.content_objects(representation):
                for generation in client.generations(content_object):
                    for bitstream in generation.bitstreams:
                        filename = bitstream.filename 
                        for algorithm,value in bitstream.fixity.items():
                            preservicadict[filename] = value    
    print('----COMPARING DICTIONARIES----')            
    diff = DeepDiff(preservicadict, droiddict, verbose_level=2)
    if len(diff) == 0:
        print('QUALITY ASSURANCE PASSED')
    else:
        with open(os.path.join(proj_path, proj_id + '_presdict.csv'), 'w', newline = '', encoding = 'utf8') as pres_dict_csv:
            pres_dict_writer = csv.writer(pres_dict_csv, delimiter=',', quotechar='"')
            for keys, values in preservicadict.items():
                pres_dict_writer.writerow([keys, values])
        with open(os.path.join(proj_path, proj_id + '_droiddict.csv'), 'w', newline = '', encoding = 'utf8') as droid_dict_csv:
            droid_dict_writer = csv.writer(droid_dict_csv, delimiter=',', quotechar='"')
            for keys, values in droiddict.items():
                droid_dict_writer.writerow([keys, values])
# quality_control_csv()

# this function moves assets from one folder to another, in this case from the OPEX folder into which assets are ingested
# and into their destination folder where they will be stored for the long-term
def move_assets():
    print('----MOVING ASSETS OUT OF OPEX INGEST FOLDER----')
    client = EntityAPI()
    folder_dest = client.folder('b3d7a581-dc77-4b98-b50b-df6504c316a0')
    count = 0
    for asset in client.all_descendants('db77b64a-64e8-4da2-9645-6f3fe92c3164'):
        client.move(asset, folder_dest)
        count += 1
        print(f'{count} - moving {asset.title}')
    print(f'moved {count} assets into {folder_dest.title}')
# move_assets()

#-------------------------------------------------------------------------------------------------------------------------------------
# GENERATING REPORTS AND STATISTICS
#-------------------------------------------------------------------------------------------------------------------------------------

# this script pulls the metadata for each ingested collection, creating a CSV file for each folder and its contents
# this is used for quality assurance by the metadata projects librarian to check the metadata post-ingest
def preservica_metadata_pull():
    print('---PULLING METADATA FOR INGESTED COLLECTIONS---')
    client = EntityAPI()
    folder_dict = {'b3d7a581-dc77-4b98-b50b-df6504c316a0':'_currents_md_export.csv', 'c1c8a2aa-3be3-4f28-a64f-fdcb3ee22314':'_univrec_md_export.csv'}
    for folder in folder_dict:
        print(f'starting metadata pull for folder ref: {folder}')
        file_name = proj_id + folder_dict[folder]
        fhand = open(os.path.join(proj_path, file_name), 'w', newline='', encoding='utf8')
        tag_list = list()
        tag_list.append('preservica id')
        csv_writer = csv.writer(fhand, delimiter=',', quotechar='"')
        count = 0
        all_assets = filter(only_assets, client.all_descendants(folder))
        for record in all_assets:
            if 'Project Documentation' in record.title:
                continue
            else:
                try:
                    xml_string = client.metadata_for_entity(record, 'http://purl.org/dc/terms/')
                    xml_tree = ET.fromstring(xml_string)
                    for elem in xml_tree.iter():
                        if elem.text != None:
                            if elem.tag not in tag_list:
                                tag_list.append(elem.tag)
                except:
                    continue
        csv_writer.writerow(tag_list)
        print(f'discovered all metadata fields for folder ref: {folder}')
        for folder in folder_dict:
            all_assets = filter(only_assets, client.all_descendants(folder))
            for record in all_assets:
                if 'Project Documentation' in record.title:
                    continue
                else:
                    try:
                        record_dict = dict()
                        xml_string = client.metadata_for_entity(record, 'http://purl.org/dc/terms/')
                        xml_tree = ET.fromstring(xml_string)
                        for elem in xml_tree.iter():
                            if elem.text != None:
                                if elem.tag not in record_dict:
                                    record_dict[elem.tag] = elem.text
                                elif elem.tag in record_dict:
                                    record_dict[elem.tag] += ' | ' + elem.text
                        record_list = list()
                        record_list.append(record.reference)
                        for tag in tag_list:
                            try:
                                record_list.append(record_dict[tag])
                            except:
                                record_list.append('')
                        csv_writer.writerow(record_list)
                        count += 1
                        print(count, record.reference)
                    except:
                        continue
        fhand.close()
        print(f'metadata extraction written to: {file_name}')
    print('METADATA EXTRACTION COMPLETE')
# preservica_metadata_pull()

#this script takes a folder ref for ingested content in Preservica and pulls out data useful
#in reporting; the script only filters on assets, and reports out the total number of assets
#in the folder, how many files are within those assets, and the total size in bytes
def report_folder():
    print('---PULLING STATS ON INGESTED FOLDER---')
    client = EntityAPI()
    assets = 0
    files = 0
    size = 0
    folder_ref = 'c04c7f5e-450d-4051-9394-82ddce47b61b'
    folder_target = client.folder(folder_ref)
    for asset in filter(only_assets, client.all_descendants(folder_target.reference)):
        if 'Project Documentation' in asset.title:
            continue
        else:
            assets += 1
            print(asset.title)
            for representation in client.representations(asset):
                for content_object in client.content_objects(representation):
                    for generation in client.generations(content_object):
                        for bitstream in generation.bitstreams:
                            files += 1
                            size += bitstream.length
            print('files:', files, 'size:', size)
    print('assets:', assets)
    print('files:', files)
    print('size:', size)
# report_folder()

#this script takes an asset ref for ingested content in Preservica and pulls out data useful
#in reporting; the script reports out how many files are within the single asset, and the total size in bytes
def report_asset():
    print('---PULLING STATS ON INGESTED ASSET---')
    client = EntityAPI()
    files = 0
    size = 0
    asset_ref = '2fc217bc-d1c2-4549-a41a-a1ff8b2595f6'
    asset = client.asset(asset_ref)
    for representation in client.representations(asset):
        for content_object in client.content_objects(representation):
            for generation in client.generations(content_object):
                for bitstream in generation.bitstreams:
                    files += 1
                    size += bitstream.length
    print(asset.title)
    print('files:', files)
    print('size:', size)
# report_asset()

#this script takes a list of asset refs for ingested content in Preservica and pulls out data useful
#in reporting; the script reports out the number of assets in the list, how many files are within all
#the assets, and the total size in bytes
def report_assets():
    print('---PULLING STATS ON INGESTED ASSETS---')
    client = EntityAPI()
    files = 0
    size = 0
    asset_total = 0
    assets = ['56a4835c-4065-4953-a071-d3142c038f87', 
              '29c581a6-0326-4997-a02f-7d304d9d1246']
    for asset_ref in assets:
        asset_total += 1
        asset = client.asset(asset_ref)
        for representation in client.representations(asset):
            for content_object in client.content_objects(representation):
                for generation in client.generations(content_object):
                    for bitstream in generation.bitstreams:
                        files += 1
                        size += bitstream.length
        print(asset.title)
    print('assets:', asset_total)
    print('files:', files)
    print('size:', size)
# annual_report_assets()


