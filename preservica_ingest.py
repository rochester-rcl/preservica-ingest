import csv
import time
import shutil
import hashlib
from pathlib import Path
from zipfile import ZipFile

start_time = time.time()
# TODO Right click on folder containing all TIFF files, 'Copy as path' and paste it here
tiff_path = Path(r"C:\sameple\path\to\tiff\files")
# TODO Right click on folder containing all PDF files, 'Copy as path' and paste it here
pdf_path = Path(r'C:\sample\path\to\pdf\files')
# TODO Right click on CSV file containing metadata, 'Copy as path' and paste it here
md_path = Path(r"C:\sample\path\to\metadata\spreadsheet.csv")
# Lines 16-17 create the 'opex' folder than all zipped PAX objects and OPEX metadata will be deposited into
upload_path = Path(r'C:\location\of\opex\folder')
upload_path.mkdir()
# Lines 19-24 look for Thumbs.db, desktop.ini, and .DS_Store files and deletes them for you
for thumbs in tiff_path.rglob('Thumbs.db'):
    thumbs.unlink()
for desktop in tiff_path.rglob('desktop.ini'):
    desktop.unlink()
for dsstore in tiff_path.rglob('*DS_Store'):
    dsstore.unlink()
# Lines 26-31 looks through everything in the folder containing the TIFF files, finds each TIFF file, then adds the parent folder to a list if it isn't already in the list
asset_dirs = list()
tiff_files = tiff_path.rglob('*.tif*')
for file in tiff_files:
    if file.parent not in asset_dirs:
        asset_dirs.append(file.parent)
        print(f'added to asset folder list: {file.parent}')
# Line 33 starts looping through the list of folders that will become assets in Preservica, each of which contains all the TIFFs for the asset
for asset in asset_dirs:
# Lines 35-36 we create a new subdirectory called "Representation_Preservation" inside the folder we are currently working on
    rep_pres_path = asset.joinpath('Representation_Preservation')
    rep_pres_path.mkdir()
# Lines 38-42 we find all the TIFF files in the current folder, create a lot of new subdirectories in "Representation_Preservation" that are each named the same as the TIFF file, and then move the corresponding TIFF file into that subdirectory just for the specific file 
    asset_files = asset.glob('*.tif*')
    for tiff in asset_files:
        filedir_path = rep_pres_path.joinpath(tiff.stem)
        filedir_path.mkdir()
        shutil.move(tiff, filedir_path.joinpath(tiff.name))
# Lines 44-45 do something very similar to the above - inside the current TIFF folder we also create a subdirectory called "Representation_Access" for the PDF file
    rep_acc_path = asset.joinpath('Representation_Access')
    rep_acc_path.mkdir()
# Lines 47-50 we search the PDF directory that we defined on line 12 for a PDF that has the same name as the folder we are in, then we create a single new subdirectory for it inside "Representation_Access", then we *copy* (not move) that PDF into the new file specific subdirectory. This merges the PDF from it's home into the TIFF directory
    for pdf in pdf_path.rglob(asset.name + '.pdf'):
        pdfdir_path = rep_acc_path.join(pdf.stem)
        pdfdir_path.mkdir()
        shutil.copy2(pdf, pdfdir_path.joinpath(pdf.stem))
# Lines 52-57 take the contents of our asset folder, which contains both "Representation" subdirectories which contain 1 PDF and many TIFFs and puts them into a new zip file within our "opex" folder - the zip file is named the same as our asset folder with a file extension of ".pax.zip" at the end. We now have our zipped PAX objects
    pax_path = upload_path.joinpath(asset.name + '.pax.zip')
    pax_obj = ZipFile(pax_path, 'w')
    for entity in asset.rglob('*'):
        pax_obj.write(entity, arcname = entity.relative_to(asset))
    pax_obj.close()
    print(f'created zipped PAX object: {pax_path}')
# Line 59 now starts looping through all of the newly created zip files from the previous section - Line 60 pulls the "title" field by grabbing the zip files's file name - Line 61 is whatever security tag you would like to apply to the resource
for pax_obj in upload_path.glob('*'):
    title = str(pax_obj.name).split('.')[0]
    sec_tag = 'public'
# Lines 63-66 open the zip file, generate a SHA1 checksum for it, store it in a variable, and close the zip file again
    pax_hand = open(pax_obj, 'rb')
    pax_read = pax_hand.read()
    sha1_checksum = hashlib.sha1(pax_read).hexdigest()
    pax_hand.close()
# Line 68 starts working on the Dublin Core metadata, creating a string that we will keep adding to which starts with the XML record header
    dublin_core = '\n\t\t<oai_dc:dc xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
# Lines 70-73 opens up the metadata spreadsheet we identified on line 14, creates a CSV reader to analyze it, and then starts looping through every line in the spreadsheet. Since the first column of the spreadsheet contains the same number that is both the asset folder name, as well as our zipped PAX object name, when we match the "title" we identified on line 60 to a line in the spreadsheet, we know we've found a match and use that line to generate the metadata
    with open(md_path, 'r', encoding='utf8', newline='') as md:
        csv_reader = csv.reader(md, quotechar='"', delimiter=',')
        for row in csv_reader:
            if row[0] == title:
# Lines 75-104 are all pretty identical, for a given column of the spreadsheet, first we check to see if the cell is empty or not, and if it contains data, then we add a line to the running Dublin Core record started on line 68 with the relevant field, then on line 105 we add the closing tag for the Dublin Core record and now our descriptive metadata is done
                if row[14] != '':
                    dublin_core += f'\n\t\t\t<dc:title>{row[14]}</dc:title>'
                if row[9] != '':
                    dublin_core += f'\n\t\t\t<dc:title>{row[10]}</dc:title>'
                if row[3] != '':
                    dublin_core += f'\n\t\t\t<dc:creator>{row[3]}</dc:creator>'
                if row[13] != '':
                    dublin_core += f'\n\t\t\t<dc:subject>{row[14]}</dc:subject>'
                if row[5] != '':
                    dublin_core += f'\n\t\t\t<dc:description>{row[5]}</dc:description>'
                if row[10] != '':
                    dublin_core += f'\n\t\t\t<dc:publisher>{row[11]}</dc:publisher>'
                if row[1] != '':
                    dublin_core += f'\n\t\t\t<dc:contributor>{row[1]}</dc:contributor>'
                if row[4] != '':
                    dublin_core += f'\n\t\t\t<dc:date>{row[4]}</dc:date>'
                if row[15] != '':
                    dublin_core += f'\n\t\t\t<dc:type>{row[16]}</dc:type>'
                if row[6] != '':
                    dublin_core += f'\n\t\t\t<dc:format>{row[6]}</dc:format>'
                if row[6] != '':
                    dublin_core += f'\n\t\t\t<dc:format>{row[7]}</dc:format>'
                if row[7] != '':
                    dublin_core += f'\n\t\t\t<dc:identifier>{row[8]}</dc:identifier>'
                if row[12] != '':
                    dublin_core += f'\n\t\t\t<dc:source>{row[13]}</dc:source>'
                if row[8] != '':
                    dublin_core += f'\n\t\t\t<dc:language>{row[9]}</dc:language>'
                if row[2] != '':
                    dublin_core += f'\n\t\t\t<dc:coverage>{row[2]}</dc:coverage>'
                if row[11] != '':
                    dublin_core += f'\n\t\t\t<dc:rights>{row[12]}</dc:rights>'
    dublin_core += '\n\t\t</oai_dc:dc>\n\t'
# Lines 107-119 are a template for the OPEX metadata that we start plugging values into, including the title, the security tag, the checksum, and the entire Dublin Core record - OPEX metadata can be a wrapper for whole other metadata schemas
    opex = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.0">
    <opex:Properties>
        <opex:Title>{title}</opex:Title>
        <SecurityDescriptor>{sec_tag}</SecurityDescriptor> 
    </opex:Properties>
    <opex:Transfer>
        <opex:Fixities>
            <opex:Fixity type="SHA-1" value="{sha1_checksum}"/>
        </opex:Fixities>
    </opex:Transfer>
    <opex:DescriptiveMetadata>{dublin_core}</opex:DescriptiveMetadata>
</opex:OPEXMetadata>'''
# Lines 121-123 open up a new file that is named the same as the zip file, except with ".opex" on the very end to indicate that this is the metadata for that zip file, and then writes the data into it
    opex_path = upload_path.joinpath(title + '.pax.zip.opex')
    opex_path.write_text(opex, encoding='utf8', newline='')
    print(f'created OPEX metadata: {opex_path}')
# Line 8 established a start time for the project, while lines 125-127 create the end time and figure out how much time has elapsed, then prints that out in minutes to the console
end_time = time.time()
time_total = round((end_time - start_time) / 60)
print(f'Total Processing Time: {time_total} mins')
