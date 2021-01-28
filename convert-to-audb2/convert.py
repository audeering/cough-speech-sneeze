import os
import shutil
import typing
import zipfile

import pandas as pd

import audb
import audb2
import audeer
import audfactory
import audformat


ROOT = 'build'
NUM_WORKERS = 8


def main():

    name = get_database_name()
    group_id = f'com.audeering.data.{name}'

    root = audeer.safe_path(ROOT)
    if os.path.exists(root):
        shutil.rmtree(root)
    root = audeer.mkdir(ROOT)

    versions = audfactory.versions(group_id, name)
    for version in versions:
        # Use try-except for loading as it might fail for broken versions
        try:
            db_original = audb_load_original_to(root, group_id, name, version)
        except RuntimeError:
            continue

        access = audb.info.access(name, group_id=group_id, version=version)
        if access == 'private':
            repository = 'data-private-local'
        else:
            repository = 'data-public-local'
        db = audformat.Database.load(root)
        if len(db.files) == 0:
            print('Skip: no files in database')
            continue

        mapping = get_archive_mapping(group_id, name, version)
        audb2.publish(
            root,
            version,
            repository,
            archives=mapping,
            verbose=True,
            num_workers=NUM_WORKERS,
        )

        check_published_database(db_original, name, version)


def audb_load_original_to(root, group_id, name, version):
    r"""Load database to cache and move to folder."""
    db = audb.load(
        name,
        group_id=group_id,
        version=version,
        keep_archive=True,
        full_path=False,
        num_jobs=NUM_WORKERS,
    )
    db_root = db.meta['audb']['root']
    # Ensure database name is correct
    if db.name != name:
        print(f'Correct old name {db.name}')
        db.name = name
    # As all the build-in solutions failed for me
    os.system(f'cp -rf {db_root}/* {root}')
    # Store as CSV
    db.save(root, num_workers=NUM_WORKERS)
    return db


def check_published_database(db_original, name, version):
    db = audb2.load(
        name,
        version=version,
        full_path=False,
        num_workers=NUM_WORKERS,
    )
    db.meta = {}
    db_original.meta = {}
    assert str(db) == str(db_original)
    assert len(db.files) == len(db_original.files)
    for table in db.tables:
        pd.testing.assert_frame_equal(
            db[table].df,
            db_original[table].df
        )


def get_archive_mapping(group_id, name, version) -> typing.Dict:
    r"""Get a list of which database files belong to which ZIP files.

    It checks which ZIP files are dependencies of the original database,
    looks for them in the local cache,
    and extracts the list of files inside every archive.

    Args:
        version: version of database

    Returns:
        mapping of files to archive

    """
    data_deps = audb.core.api._get_dependencies(
        group_id,
        f'{name}-data',
        version,
        transitive=True,
        verbose=False,
    )
    artifact_urls = audfactory.list_artifacts(
        data_deps,
        repository='data-local',
    )
    archive_root = os.path.join(
        audb.get_default_cache_root(),
        audb.config.AUDB_ARCHIVE_NAME,
    )
    archive_root = audeer.safe_path(archive_root)
    zip_files = [u.split('data-local/')[-1] for u in artifact_urls]
    zip_files = [os.path.join(archive_root, f) for f in zip_files]

    mapping = {}
    for zip_file in zip_files:
        # List of files in ZIP file
        zf = zipfile.ZipFile(zip_file)
        files = [z.filename for z in zf.infolist()]
        # Map every file to ZIP archive name without version
        for file in files:
            name = audeer.basename_wo_ext(zip_file)
            name_wo_version = '-'.join(name.split('-')[:-1])
            mapping[file] = name_wo_version

    return mapping


def get_database_name():
    r"""Get database name from parent folder."""
    current_file = audeer.safe_path(__file__)
    current_dir = os.path.dirname(current_file)
    parent_dir = os.path.dirname(current_dir)
    return os.path.basename(parent_dir)


if __name__ == '__main__':
    main()
