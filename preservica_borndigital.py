from pathlib import Path
import hashlib
import uuid

print('Right click on the folder containing the files you want to process and click the "Copy as path" option, then enter that into the following prompt')
proj_path = input('Please input the path to your project folder: ')
proj_path = proj_path.replace('"', '')
proj_path = Path(proj_path)
proj_files = proj_path.rglob('*')
for entity in proj_files:
    if entity.suffix == '.opex':
        continue
    elif 'archival_object_' in entity.name:
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
</opex:OPEXMetadata>'''.format(ao_num = entity.name)
        with open(entity.joinpath(entity.name + '.opex'), 'w', encoding='utf8') as ao_opex:
            ao_opex.write(opex)
        print('created: ', entity.name + '.opex')
    elif entity.is_file():
        fhand = open(entity, 'rb')
        fread = fhand.read()
        checksum = hashlib.sha1(fread).hexdigest()
        fhand.close()
        opex = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.0">
    <opex:Properties>
        <opex:Title>{title}</opex:Title>
        <SecurityDescriptor>public</SecurityDescriptor> 
    </opex:Properties>
    <opex:Transfer>
        <opex:Fixities>
            <opex:Fixity type="SHA-1" value="{checksum}"/>
        </opex:Fixities>
    </opex:Transfer>
</opex:OPEXMetadata>'''.format(title=entity.stem, checksum=checksum)
        with open(entity.parent.joinpath(entity.name + '.opex'), 'w', encoding='utf8') as file_opex:
            file_opex.write(opex)
        print('created: ', entity.name + '.opex')
    elif entity.is_dir():
        code = uuid.uuid4()
        file_manifest = ''
        for file in entity.glob('*'):
            file_size = file.stat().st_size
            file_name = file.name
            if file.suffix == '.opex':
                continue
            else:
                file_manifest += f'\t\t\t<opex:File type="content" size="{file_size}">{file_name}</opex:File>\n'
        opex = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.0">
    <opex:Properties>
        <opex:Title>{title}</opex:Title>
        <opex:Identifiers>
            <opex:Identifier type="code">{code}</opex:Identifier>
        </opex:Identifiers>
        <SecurityDescriptor>public</SecurityDescriptor> 
    </opex:Properties>
    <opex:Transfer>
        <opex:Files>
{file_manifest}\t\t</opex:Files>
    </opex:Transfer>
    <opex:DescriptiveMetadata>
        <LegacyXIP xmlns="http://preservica.com/LegacyXIP">
            <AccessionRef>catalogue</AccessionRef>
        </LegacyXIP>
    </opex:DescriptiveMetadata>
</opex:OPEXMetadata>'''.format(title=entity.name, code=code, file_manifest=file_manifest)
        with open(entity.joinpath(entity.name + '.opex'), 'w', encoding='utf8') as folder_opex:
            folder_opex.write(opex)
        print('created: ', entity.name + '.opex')
