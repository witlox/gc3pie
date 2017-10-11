import argparse
import bz2
import csv
import json
import os
import tarfile
import time
from multiprocessing import Manager, Process, current_process, cpu_count
from zipfile import ZipFile, is_zipfile


class Decoder(object):

    def __init__(self):
        self._dialect = None
        self._data_type = None

    def _decode_csv_(self, data):
        for line in csv.reader(data.splitlines(), delimiter=self._dialect.delimiter, quotechar=self._dialect.quotechar):
            yield line

    def _decode_json_(self, data):
        try:
            for o in json.loads(data):
                yield o
        except ValueError:
            pass
        for line in data.splitlines():
            try:
                for o in json.loads(line):
                    yield o
            except ValueError:
                pass

    def _detect_data_type_(self, data):
        try:
            json.loads(data)
            return 'json'
        except ValueError:
            try:
                json.loads(data.split('\n')[0])
                return 'json'
            except ValueError:
                pass
        lines = data.split('\n')
        ins = len(lines) if len(lines) <= 10 else 10
        sniffer = csv.Sniffer()
        self._dialect = sniffer.sniff(lines[0])
        for line in lines[1:ins]:
            if line.startswith("#"):
                if ins == len(lines):
                    return 'text'
                ins += 1
                continue
            if sniffer.sniff(line).delimiter != self._dialect.delimiter \
                    or sniffer.sniff(line).quotechar != self._dialect.quotechar:
                return 'text'
        return 'csv'

    def decode(self, data):
        result = []
        if not self._data_type:
            self._data_type = self._detect_data_type_(data)
        if self._data_type == 'csv':
            for line in self._decode_csv_(data):
                result.append(line)
        elif self._data_type == 'json':
            for line in self._decode_json_(data):
                result.append(line)
        return result


def extract_file(data_file):
    """
     Convert raw 'data' file to actual data 
     :param data_file: data file
     :return: data
     """
    print('parsing file (size: {0})'.format(len(data_file)))
    try:
        data_file = bz2.decompress(data_file)
        print('decompressed data to {0}'.format(len(data_file)))
    except IOError:
        pass
    return Decoder().decode(data_file)


def post_data_to_writer(data, wq, ot):
    if ot == 'csv':
        for row in data:
            wq.put(';'.join(row))
    elif ot == 'json':
        wq.put(json.dumps(dict(data)))
    else:
        wq.put(str(data))


def extract_tar_file(archive_file, ap, wq, ot):
    print('going to extract archive file {0} from tar {1}'.format(archive_file.name, ap))
    with tarfile.open(ap) as tf:
        extracted_archive_file = tf.extractfile(archive_file)
        if not extracted_archive_file.closed:
            file_data = extracted_archive_file.read()
            data = extract_file(file_data)
            extracted_archive_file.close()
            post_data_to_writer(data, wq, ot)
        else:
            print('file pointer not opened, something went wrong for {0} in {1}, skipping..,'.format(archive_file.name, ap))


def extract_zip_file(archive_file, ap, wq, ot):
    print('going to extract archive file {0} from zip {1}'.format(archive_file, ap))
    with ZipFile(ap) as zf:
        file_data = zf.read(archive_file)
        data = extract_file(file_data)
        post_data_to_writer(data, wq, ot)


def process_archive(ap, fq, wq, ot):
    """
    Worker that processes a single file from the archive   
    :param ap: name of the archive
    :param fq: queue to read from
    :param wq: queue to write to
    :param ot: output type
    """
    print('Starting process_archive indexer named {0}'.format(current_process().name))
    while True:
        archive_file = fq.get()
        if isinstance(archive_file, basestring) and archive_file == 'EOP':
            break
        else:
            if is_zipfile(ap):
                extract_zip_file(archive_file, ap, wq, ot)
            elif tarfile.is_tarfile(ap):
                extract_tar_file(archive_file, ap, wq, ot)


def mp_writer(o, wq, compress):
    """
    Multiprocess listener. Listens to message on Queue and writes them to file
    :param o: name of the output file
    :param wq: queue to read from 
    :param compress: enable or disable compression 
    """
    print('setting up writer for {0} (compression enabled: {1})'.format(o, compress))
    if compress:
        with bz2.BZ2File(o, 'wb') as fp:
            while True:
                m = wq.get()
                if m == 'EOP':
                    break
                fp.write('{0}\n'.format(m))
    else:
        with open(o, 'wb') as fp:
            while True:
                m = wq.get()
                if m == 'EOP':
                    fp.flush()
                    break
                fp.write('{0}\n'.format(m))
                fp.flush()


def queue_archive_files(ap, fq):
    """
    Extract all data and process everything :)
    :param ap: name of the archive
    :param fq: puts the file names on the queue 
    """
    if is_zipfile(ap):
        with ZipFile(ap, 'r') as archive:
            for archive_file in archive.namelist():
                if not archive_file.endswith('/') and ZipFile.getinfo(archive, archive_file).file_size > 0:
                    fq.put(archive_file)
    elif tarfile.is_tarfile(ap):
        with tarfile.open(ap) as archive:
            for archive_file in archive:
                if archive_file.isreg() and archive_file.size > 0:
                    fq.put(archive_file)
    else:
        raise IOError('could not match {0} to zip or tar format'.format(ap))


if __name__ == "__main__":
    # wait {timeout} seconds for our file to become available
    parser = argparse.ArgumentParser(description='Extract data from zip/tar and parse into a single file')
    parser.add_argument('-a', '--archive', help='path to the archive', required=True)
    parser.add_argument('-p', '--processors', help='amount of file processors to spin up, '
                                                  'default: %(default)d.', type=int, default=cpu_count())
    parser.add_argument('-t', '--time-out', help='waiting time in seconds for the archive to become available, '
                                                 'default: %(default)d.', type=int, default=360)
    parser.add_argument('-f', '--output-format', help='format in which to write the output file (csv, json, text), '
                                                      'default: %(default)s.', default='csv')
    parser.add_argument('-n', '--no-compression', help='do not compress output file as bz2', action='store_true')
    parser.add_argument('-o', '--output', help='directory output path of the single columnar file')
    args = parser.parse_args()

    archive_path = os.path.expanduser(os.path.expandvars(args.archive))
    timeout = args.time_out
    while not os.path.exists(archive_path):
        timeout -= 1
        if timeout > 0:
            print('file {0} not available yet, waiting {1} more seconds'.format(archive_path, timeout))
        else:
            print('time out exceeded, terminating program, please check configuration for issues')
            exit(-2)
        time.sleep(1)

    print('setting up multiprocessing')
    manager = Manager()
    file_queue = manager.Queue()
    writer_queue = manager.Queue()
    indexer = Process(target=queue_archive_files, args=(archive_path, file_queue,))
    indexer.start()
    output_file_ext = '{of}.bz2'.format(of=args.output_format)
    if args.no_compression:
        output_file_ext = args.output_format
    output_file = '{fn}.{ext}'.format(fn=os.path.splitext(os.path.basename(args.archive))[0], ext=output_file_ext)
    if args.output:
        output_file = os.path.join(args.output, '{fn}.{ext}'.format(fn=os.path.basename(args.archives),
                                                                    ext=output_file_ext))
    compression = True if not args.no_compression else False
    writer = Process(target=mp_writer, args=(output_file, writer_queue, compression,))
    writer.start()
    print('Setup file processors and start crunching')
    jobs = []
    for index in range(0, args.processors):
        jobs.append(Process(target=process_archive, args=(archive_path, file_queue, writer_queue, args.output_format)))
    for job in jobs:
        job.start()
        print('Started process {0} in pid {1}'.format(job.name, job.pid))
    print('wait for our indexer to finish')
    indexer.join()
    print('add terminators to index file queue (as last element)')
    for job in jobs:
        file_queue.put('EOP')
    print('wait for our processors to finish')
    for job in jobs:
        job.join()
    print('and now add terminator for writer and wait to finish')
    writer_queue.put('EOP')
    writer.join()
    print('Elvis has left the building')
