"""Offline tests for the actual ingestion functions (no BLS/API/network access)."""

import json
from pathlib import Path

import pytest

from ingestion.ingest import POP_URL, BLS_URL, load_manifest, run_ingestion


class FakeResponse:
    def __init__(self, url, content):
        self.url = url
        self.content = content
        self.text = content.decode('utf-8')

    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size):
        for offset in range(0, len(self.content), chunk_size):
            yield self.content[offset:offset + chunk_size]

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class FakeSession:
    def __init__(self, files):
        self.files = files
        self.fail_population = False

    def get(self, url, **kwargs):
        if url == BLS_URL:
            links = ''.join(f'<a href="{name}">{name}</a>' for name in sorted(self.files))
            return FakeResponse(url, links.encode('utf-8'))
        if url == POP_URL:
            if self.fail_population:
                return FakeResponse(url, b'{"data": []}')
            return FakeResponse(url, json.dumps({'data': [
                {'Year': 2024, 'Nation': 'United States', 'Population': 340110990}
            ]}).encode('utf-8'))
        name = url.removeprefix(BLS_URL)
        if name in self.files and url.startswith(BLS_URL):
            return FakeResponse(url, self.files[name])
        raise AssertionError(f'Unexpected HTTP request: {url}')


@pytest.fixture
def fake_session():
    return FakeSession({
        'pr.data.1.AllData': b'series_id\tyear\tperiod\tvalue\nTEST001\t2024\tQ01\t1.0\n',
        'pr.series': b'series_id\tseries_title\nTEST001\tOriginal title\n',
    })


def snapshots(volume):
    return sorted((volume / 'manifests').glob('snapshot_*.json'))


def by_name(manifest, filename):
    return next(item for item in manifest['active_files'] if item['filename'] == filename)


def test_initial_and_repeat_no_new_snapshots(tmp_path, fake_session):
    first = run_ingestion(fake_session, tmp_path)
    assert len(first['active_files']) == 3
    assert len(snapshots(tmp_path)) == 1
    second = run_ingestion(fake_session, tmp_path)
    assert len(snapshots(tmp_path)) == 1
    assert all(status == 'unchanged' for status in second['actions'].values())


def test_modified_file_creates_version_and_retains_old(tmp_path, fake_session):
    run_ingestion(fake_session, tmp_path)
    previous = by_name(load_manifest(tmp_path / 'manifests'), 'pr.series')
    fake_session.files['pr.series'] = b'series_id\tseries_title\nTEST001\tNew title\n'
    run_ingestion(fake_session, tmp_path)
    current = by_name(load_manifest(tmp_path / 'manifests'), 'pr.series')
    assert previous['sha256'] != current['sha256']
    assert Path(previous['volume_path']).is_file()
    assert Path(current['volume_path']).is_file()
    assert len(snapshots(tmp_path)) == 2


def test_added_file_is_discovered(tmp_path, fake_session):
    run_ingestion(fake_session, tmp_path)
    fake_session.files['pr.measure'] = b'measure_code\tmeasure_text\n01\tExample\n'
    run_ingestion(fake_session, tmp_path)
    assert len(load_manifest(tmp_path / 'manifests')['active_files']) == 4


def test_removed_file_is_inactive_but_historically_preserved(tmp_path, fake_session):
    fake_session.files['pr.measure'] = b'code\tname\n01\tExample\n'
    run_ingestion(fake_session, tmp_path)
    previous = by_name(load_manifest(tmp_path / 'manifests'), 'pr.measure')
    del fake_session.files['pr.measure']
    run_ingestion(fake_session, tmp_path)
    current = load_manifest(tmp_path / 'manifests')
    assert len(current['active_files']) == 3
    assert {'source': 'bls', 'filename': 'pr.measure'} in current['removed_since_previous']
    assert Path(previous['volume_path']).is_file()


def test_failure_does_not_publish_partial_manifest(tmp_path, fake_session):
    run_ingestion(fake_session, tmp_path)
    first_hashes = {(r['source'], r['filename'], r['sha256']) for r in
                    load_manifest(tmp_path / 'manifests')['active_files']}
    fake_session.files['pr.series'] = b'new content'
    fake_session.fail_population = True
    with pytest.raises(ValueError, match='Population API'):
        run_ingestion(fake_session, tmp_path)
    assert len(snapshots(tmp_path)) == 1
    assert {(r['source'], r['filename'], r['sha256']) for r in
            load_manifest(tmp_path / 'manifests')['active_files']} == first_hashes


def test_empty_directory_refuses_publish(tmp_path):
    with pytest.raises(ValueError, match='no files'):
        run_ingestion(FakeSession({}), tmp_path)
    assert len(snapshots(tmp_path)) == 0


def test_private_contact_configuration(tmp_path):
    from ingestion.ingest import load_contact_email
    config = tmp_path / 'config' / 'local.json'
    config.parent.mkdir(parents=True)
    config.write_text(json.dumps({'bls_contact_email': 'test@example.org'}))
    assert load_contact_email(tmp_path, supplied_email=None) == 'test@example.org'
