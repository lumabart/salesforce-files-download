import os
import re
import csv
import logging
import threading
import requests
from concurrent.futures import ThreadPoolExecutor
from simple_salesforce import Salesforce

# global locks for thread-safe writes
csv_writer_lock = threading.Lock()
error_file_lock = threading.Lock()


def split_into_batches(items, batch_size):
    full_list = list(items)
    for i in range(0, len(full_list), batch_size):
        yield full_list[i:i + batch_size]


def create_filename(title, file_extension, content_document_id,
                    output_directory, parent_object_id, grouping_folder, filename_pattern):
    
    # sanitize title
    if os.name == 'nt':
        bad_chars = re.compile(r'[^A-Za-z0-9_. ]+|^\.|\.$|^ | $|^$')
        bad_names = re.compile(r'(aux|com[1-9]|con|lpt[1-9]|prn)(\.|$)', re.IGNORECASE)
        clean_title = bad_chars.sub('_', title)
        if bad_names.match(clean_title):
            clean_title = '_' + clean_title
    else:
        bad_chars = [';', ':', '!', '*', '/', '\\']
        clean_title = ''.join(ch for ch in title if ch not in bad_chars)

    return filename_pattern.format(
        output_directory.rstrip(os.sep) + os.sep,
        content_document_id,
        clean_title,
        file_extension,
        parent_object_id,
        grouping_folder
    )


def download_file(args):
    record, output_directory, sf, results_path, content_document_links, \
        content_document_id_name, filename_pattern, error_file_path = args

    try:
        
        content_document_id = record["ContentDocumentId"]
        title = record["Title"]
        file_extension = record["FileExtension"]
        

        # find the matching link for parent entity
        cdl = next(
            (cdl for cdl in content_document_links
             if cdl[content_document_id_name] == content_document_id),
            {}
        )
        linked_entity_id = cdl.get("LinkedEntityId", cdl.get("Id", ""))
        linked_entity_name = (
            cdl.get("LinkedEntity", {}).get("Name", cdl.get("Title", ""))
        )

        grouping_folder = linked_entity_name + '-' + linked_entity_id + os.sep

        url = f"https://{sf.sf_instance}{record['VersionData']}"
        logging.debug(f"Downloading from {url}")

        response = requests.get(
            url,
            headers={
                "Authorization": f"OAuth {sf.session_id}",
                "Content-Type": "application/octet-stream"
            }
        )

        if not response.ok:
            err_msg = f"Couldn't download {url} (status {response.status_code})"
            with error_file_lock:
                with open(error_file_path, 'a', encoding='utf-8') as ef:
                    ef.write(f"{content_document_id},{linked_entity_id},"
                             f"{linked_entity_name},{err_msg}\n")
            return err_msg

        # ensure output directory exists
        os.makedirs(output_directory + grouping_folder, exist_ok=True)
        filename = create_filename(
            title, file_extension, content_document_id,
            output_directory, linked_entity_id, grouping_folder,
            filename_pattern
        )
        logging.debug(f"Saving file to {filename!r}")

        try:
            with open(filename, "wb") as output_file:
                output_file.write(response.content)
        except PermissionError as e:
            err_msg = f"Permission denied when writing {filename!r}: {e}"
            logging.error(err_msg)
            with error_file_lock:
                with open(error_file_path, 'a', encoding='utf-8') as ef:
                    ef.write(f"{content_document_id},{linked_entity_id},"
                             f"{linked_entity_name},PermissionError\n")
            return err_msg

        # write entry to CSV
        with csv_writer_lock:
            with open(results_path, 'a', encoding='utf-8', newline='') as results_csv:
                writer = csv.writer(results_csv, delimiter=',',
                                    quotechar='|', quoting=csv.QUOTE_MINIMAL)
                writer.writerow([
                    linked_entity_id,
                    linked_entity_name,
                    content_document_id,
                    title,
                    filename,
                    filename
                ])

        return f"Saved file to {filename}"

    except Exception as e:
        err_msg = f"Exception for {record.get('ContentDocumentId', 'unknown')}: {e}"
        logging.exception(err_msg)
        with error_file_lock:
            with open(error_file_path, 'a', encoding='utf-8') as ef:
                ef.write(f"{record.get('ContentDocumentId','')},"
                         f"{record.get('LinkedEntityId','')},"
                         f"{record.get('Title','')},Exception,{e}\n")
        return err_msg


def fetch_files(sf, content_document_links=None,
                output_directory=None, results_path=None,
                filename_pattern=None,
                content_document_id_name='ContentDocumentId',
                batch_size=100,
                error_file_path=None):

    os.makedirs(output_directory, exist_ok=True)
    batches = list(split_into_batches(content_document_links, batch_size))
    logging.info(f"Processing {len(batches)} batches")

    for idx, batch in enumerate(batches, start=1):
        logging.info(f"Processing batch {idx}/{len(batches)}")

        ids_csv = ",".join(f"'{item[content_document_id_name]}'" for item in batch)
        query = (
            "SELECT ContentDocumentId, Title, VersionData, FileExtension "
            "FROM ContentVersion "
            "WHERE IsLatest = True AND FileExtension != 'snote' "
            f"AND ContentDocumentId IN ({ids_csv})"
        )
        resp = sf.query(query)
        records = resp.get("records", [])
        logging.debug(f"Found {len(records)} files to download in this batch")

        # workers number to
        with ThreadPoolExecutor(max_workers=50) as executor:
            args_iter = (
                (rec, output_directory, sf, results_path,
                 batch, content_document_id_name, filename_pattern, error_file_path)
                for rec in records
            )
            for result in executor.map(download_file, args_iter):
                logging.debug(result)

    logging.info("All batches complete.")


def main():
    import argparse
    import configparser

    parser = argparse.ArgumentParser(
        description='Export ContentVersion (Files) from Salesforce'
    )
    parser.add_argument('-q', '--query', required=True,
                        help='SOQL to limit the valid ContentDocumentIds.')
    parser.add_argument('-d', '--subdirectory', required=True,
                        help='Subdirectory under the main output directory')
    parser.add_argument('-o', '--object', default='ContentDocumentLink',
                        choices=['ContentDocumentLink', 'ContentDocument'],
                        help='Source object for selecting documents')
    parser.add_argument('-f', '--filenamepattern',
                        default='{0}{5}{4}-{1}-{2}.{3}',
                        help='Filename pattern: {0}=output_dir, {1}=docId, '
                             '{2}=title, {3}=ext, {4}=parentId, {5}=grouping_by_parent_folder')
    args = parser.parse_args()

    # read settings
    config = configparser.ConfigParser(allow_no_value=True)
    config.read('download.ini')
    sf_cfg = config['salesforce']

    domain = sf_cfg.get('domain', '')
    if sf_cfg.getboolean('connect_to_sandbox', fallback=False):
        domain = domain + '.test' if domain else 'test'
    domain = (domain + '.my') if domain else 'login'

    output_directory = os.path.join(
        sf_cfg.get('output_dir', '.'), args.subdirectory
    ) + os.sep
    batch_size = sf_cfg.getint('batch_size', fallback=100)
    loglevel = getattr(logging, sf_cfg.get('loglevel', 'INFO').upper(), logging.INFO)

    # prepare logging
    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(message)s',
        level=loglevel
    )
    logging.info(f"Export ContentVersion (Files) from Salesforce at https://{domain}.salesforce.com")

    # prepare result & error files
    os.makedirs(output_directory, exist_ok=True)
    results_path = os.path.join(output_directory, 'files.csv')
    error_file_path = os.path.join(output_directory, 'errors.txt')

    # initialize or preserve files.csv
    if not os.path.exists(results_path):
        with open(results_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            writer.writerow([
                'FirstPublicationId',
                'FirstPublicationName',
                'ContentDocumentId',
                'Title',
                'VersionData',
                'PathOnClient'
            ])

    # clear or create error file
    with open(error_file_path, 'w', encoding='utf-8') as ef:
        ef.write('')  # truncate existing

    # connect to Salesforce
    sf = Salesforce(
        username=sf_cfg['username'],
        password=sf_cfg['password'],
        security_token=sf_cfg['security_token'],
        domain=domain
    )

    # query for ContentDocumentLinks
    if args.object == 'ContentDocumentLink':
        cdl_query = (
            'SELECT ContentDocumentId, LinkedEntityId, LinkedEntity.Name, '
            'ContentDocument.Title, ContentDocument.FileExtension '
            f'FROM ContentDocumentLink WHERE ContentDocument.FileType != \'SNOTE\' AND LinkedEntityId IN ({args.query})'
        )
        content_document_id_name = 'ContentDocumentId'
    else:
        cdl_query = (
            'SELECT Id AS ContentDocumentId, Title, FileExtension '
            f'FROM ContentDocument {args.query}'
        )
        content_document_id_name = 'ContentDocumentId'

    logging.info("Querying for ContentDocumentLinks...")
    content_document_links = sf.query_all(cdl_query)['records']
    logging.info(f"Found {len(content_document_links)} total ContentDocumentLinks")

    # filter out already downloaded
    downloaded_ids = set()
    with open(results_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=',', quotechar='|')
        for row in reader:
            downloaded_ids.add(row['ContentDocumentId'])

    before = len(content_document_links)
    content_document_links = [
        link for link in content_document_links
        if link[content_document_id_name] not in downloaded_ids
    ]
    logging.info(f"Skipped {before - len(content_document_links)} already-downloaded files, "
                 f"{len(content_document_links)} remain.")

    # start downloads
    fetch_files(
        sf=sf,
        content_document_links=content_document_links,
        output_directory=output_directory,
        results_path=results_path,
        filename_pattern=args.filenamepattern,
        content_document_id_name=content_document_id_name,
        batch_size=batch_size,
        error_file_path=error_file_path
    )
    # wywolanie dla orders: python download_order.py -q "Select Id From Order" -d order

if __name__ == "__main__":
    main()
