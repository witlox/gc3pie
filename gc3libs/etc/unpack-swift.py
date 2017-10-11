#!/usr/bin/python
# Extract an archive and write the contents of it to Swift Object Storage
import argparse
import os
import tarfile
import time
from multiprocessing import Manager, Process, current_process, cpu_count
from zipfile import ZipFile, is_zipfile

import swiftclient


def extract_tar_file(archive_file, ap):
    print('going to extract archive file {0} from tar {1}'.format(archive_file.name, ap))
    with tarfile.open(ap) as tf:
        extracted_archive_file = tf.extractfile(archive_file)
        if not extracted_archive_file.closed:
            yield extracted_archive_file.read()
            extracted_archive_file.close()
        else:
            print('file pointer not opened, something went wrong for {0} in {1}, skipping..,'.format(archive_file.name, ap))


def extract_zip_file(archive_file, ap):
    print('going to extract archive file {0} from zip {1}'.format(archive_file, ap))
    with ZipFile(ap) as zf:
        yield zf.read(archive_file)


def process_archive(ap, fq, sc, con, pf):
    """
    Worker that processes a single file from the archive   
    :param ap: name of the archive
    :param fq: queue to read from
    :param sc: swift connection data 
    :param con: container 
    :param pf: prefix 
    """
    print('Starting process_archive indexer named {0}'.format(current_process().name))
    with swiftclient.client.Connection(authurl=sc['auth'],
                                       user=sc['user'],
                                       key=sc['sess'],
                                       tenant_name=sc['proj'],
                                       auth_version='2.0') as swift_conn:
        while True:
            archive_file = fq.get()
            if isinstance(archive_file, basestring) and archive_file == 'EOP':
                break
            else:
                if is_zipfile(ap):
                    data = extract_zip_file(archive_file, ap)
                else:
                    data = extract_tar_file(archive_file, ap)
                swift_conn.put_object(con, '{prefix}{path}'.format(prefix=pf, path=archive_file), data)


def queue_archive_files(ap, fq):
    """
    Extract all archive content names and put them on a queue
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
    parser = argparse.ArgumentParser(description='Extract data from zip/tar to a Swift container.')
    parser.add_argument('-a', '--archive', help='path to the archive', required=True)
    parser.add_argument('-c', '--container', help='Swift container name', required=True)
    parser.add_argument('-n', '--num-cores', help='requested cores, %(default)d.', type=int, default=cpu_count())
    parser.add_argument('-t', '--time-out', help='waiting time in seconds for the archive to become available, '
                                                 'default: %(default)d.', type=int, default=360)
    parser.add_argument('-p', '--prefix', help='prefix for the repository root.')
    args = parser.parse_args()

    os_vars = {}
    with open('amaterial.dat', 'r') as os_file:
        for line in os_file:
            name, var = line.partition("=")[::2]
            os_vars[name.strip()] = var

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
    indexer = Process(target=queue_archive_files, args=(archive_path, file_queue,))
    indexer.start()
    print('Setup file processors {0} and start crunching'.format(args.num_cores))
    jobs = []
    for index in range(0, args.num_cores):
        jobs.append(Process(target=process_archive, args=(archive_path,
                                                          file_queue,
                                                          os_vars,
                                                          args.container,
                                                          args.prefix,)))
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
    print('Elvis has left the building')
