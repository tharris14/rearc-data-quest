"""Fetch BLS productivity source files and Data USA population into a Databricks Volume.

Run from a Databricks Python notebook or Python environment with access to the Volume:
    %run ./ingestion/ingest  # or import and call main() from a notebook
    python -m ingestion.ingest --volume /Volumes/<catalog>/<schema>/<volume>

All source files are retained as content-addressed immutable versions. A manifest
snapshot represents the active source inventory at the end of a successful run.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
import uuid
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_VOLUME = Path('/Volumes/workspace/rearc_quest/source_files')
BLS_URL = 'https://download.bls.gov/pub/time.series/pr/'
POP_URL = 'https://honolulu-api.datausa.io/tesseract/data.jsonrecords'
POP_PARAMS = {
    'cube': 'acs_yg_total_population_1',
    'drilldowns': 'Year,Nation',
    'locale': 'en',
    'measures': 'Population',
}


class DirectoryParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            href = dict(attrs).get('href')
            if href:
                self.links.append(href)


def load_contact_email(volume: Path, supplied_email: str | None = None) -> str:
    """Read non-secret BLS contact identity from argument, env, or private Volume config.

    No contact address is embedded in or logged by the codebase.
    """
    email = supplied_email or os.environ.get('BLS_CONTACT_EMAIL')
    if not email:
        config_path = volume / 'config' / 'local.json'
        if config_path.is_file():
            with config_path.open(encoding='utf-8') as f:
                email = json.load(f).get('bls_contact_email')
    if not email or '@' not in email or '\n' in email or '\r' in email:
        raise ValueError(
            'Set BLS_CONTACT_EMAIL or create <volume>/config/local.json '
            'containing {"bls_contact_email": "..."}. '
        )
    return email.strip()


def make_session(contact_email: str) -> requests.Session:
    retry = Retry(
        total=4,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=['GET'],
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.headers.update({'User-Agent': f'RearcDataQuest/1.0 (contact: {contact_email})'})
    session.mount('https://', HTTPAdapter(max_retries=retry))
    return session


def discover_bls_files(session, base_url: str = BLS_URL) -> dict[str, str]:
    """Discover direct file links dynamically; never assume a fixed filename list."""
    response = session.get(base_url, timeout=30)
    response.raise_for_status()
    parser = DirectoryParser()
    parser.feed(response.text)
    base = urlparse(base_url)
    files = {}
    for link in parser.links:
        candidate = urljoin(base_url, link)
        parsed = urlparse(candidate)
        filename = parsed.path[len(base.path):] if parsed.path.startswith(base.path) else ''
        if (parsed.scheme == 'https' and parsed.netloc == base.netloc and filename
                and '/' not in filename and not parsed.query and not parsed.fragment):
            files[filename] = candidate
    if not files:
        raise ValueError('BLS directory yielded no files; refusing to publish empty inventory')
    return dict(sorted(files.items()))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def load_manifest(manifest_dir: Path) -> dict:
    snapshots = sorted(manifest_dir.glob('snapshot_*.json'))
    if not snapshots:
        return {'active_files': []}
    # Do not silently revert to an older snapshot if the latest is invalid.
    with snapshots[-1].open(encoding='utf-8') as stream:
        manifest = json.load(stream)
    if not isinstance(manifest.get('active_files'), list):
        raise ValueError('Latest manifest lacks active_files')
    return manifest


def validate_population_json(path: Path) -> None:
    with path.open(encoding='utf-8') as stream:
        payload = json.load(stream)
    records = payload.get('data')
    required = {'Year', 'Nation', 'Population'}
    if (not isinstance(records, list) or not records or
            not all(isinstance(item, dict) and required.issubset(item) for item in records)):
        raise ValueError('Population API returned missing or malformed data')


def land_file(
    session,
    raw_dir: Path,
    source: str,
    filename: str,
    url: str,
    previous: dict,
    params: dict | None = None,
) -> tuple[dict, str]:
    """Download and checksum a file; only persist a new content version if needed."""
    temp_path = None
    try:
        with session.get(url, params=params, stream=True, timeout=(10, 90)) as response:
            response.raise_for_status()
            source_url = response.url
            with tempfile.NamedTemporaryFile(delete=False) as temp:
                temp_path = Path(temp.name)
                for chunk in response.iter_content(1024 * 1024):
                    if chunk:
                        temp.write(chunk)
        if source == 'data_usa':
            validate_population_json(temp_path)
        digest = sha256_file(temp_path)
        destination = raw_dir / source / filename / digest / filename
        old_record = previous.get((source, filename))
        same_version = old_record is not None and old_record['sha256'] == digest
        already_saved = destination.is_file() and sha256_file(destination) == digest
        if not already_saved:
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(temp_path, destination)
            if sha256_file(destination) != digest:
                raise IOError(f'SHA-256 verification failed: {filename}')
        if same_version and already_saved:
            status = 'unchanged'
        elif old_record is None:
            status = 'new'
        elif same_version:
            status = 'restored'
        else:
            status = 'updated'
        return ({
            'source': source,
            'filename': filename,
            'source_url': source_url,
            'sha256': digest,
            'volume_path': str(destination),
            'size_bytes': temp_path.stat().st_size,
        }, status)
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


def run_ingestion(session, volume: Path = DEFAULT_VOLUME, *, bls_url: str = BLS_URL,
                  pop_url: str = POP_URL, pop_params: dict | None = None) -> dict:
    """Ingest one complete source inventory; publish a manifest after every file succeeds."""
    raw_dir = volume / 'raw'
    manifest_dir = volume / 'manifests'
    raw_dir.mkdir(parents=True, exist_ok=True)
    manifest_dir.mkdir(parents=True, exist_ok=True)
    previous_manifest = load_manifest(manifest_dir)
    previous_records = previous_manifest['active_files']
    previous = {(r['source'], r['filename']): r for r in previous_records}
    current_files = []
    actions = {}

    for filename, url in discover_bls_files(session, bls_url).items():
        record, action = land_file(session, raw_dir, 'bls', filename, url, previous)
        current_files.append(record)
        actions[('bls', filename)] = action
        print(f'BLS {filename}: {action}')

    record, action = land_file(
        session, raw_dir, 'data_usa', 'population.json', pop_url, previous,
        params=POP_PARAMS if pop_params is None else pop_params,
    )
    current_files.append(record)
    actions[('data_usa', 'population.json')] = action
    print(f'Data USA population.json: {action}')

    current_set = {(r['source'], r['filename'], r['sha256']) for r in current_files}
    previous_set = {(r['source'], r['filename'], r['sha256']) for r in previous_records}
    removed = [
        {'source': source, 'filename': filename}
        for source, filename in sorted(set(previous) - {
            (r['source'], r['filename']) for r in current_files
        })
    ]
    if current_set != previous_set:
        now = datetime.now(timezone.utc)
        snapshot = {
            'run_id': str(uuid.uuid4()),
            'created_at_utc': now.isoformat(),
            'active_files': sorted(current_files, key=lambda r: (r['source'], r['filename'])),
            'removed_since_previous': removed,
        }
        destination = manifest_dir / f'snapshot_{now.strftime("%Y%m%dT%H%M%S%fZ")}.json'
        temp_snapshot = None
        try:
            # Publish only after building a complete, parseable inventory.
            with tempfile.NamedTemporaryFile(
                mode='w', encoding='utf-8', suffix='.json',
                prefix='publishing_', dir=manifest_dir, delete=False
            ) as stream:
                json.dump(snapshot, stream, indent=2)
                temp_snapshot = Path(stream.name)
            os.replace(temp_snapshot, destination)
        finally:
            if temp_snapshot is not None:
                temp_snapshot.unlink(missing_ok=True)
        print(f'Manifest created: {destination.name}')
    else:
        print('No source changes. Existing manifest retained.')
    print(f'Active files: {len(current_files)}; removed files: {len(removed)}')
    return {'actions': actions, 'active_files': current_files, 'removed': removed}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--volume', type=Path, default=DEFAULT_VOLUME)
    parser.add_argument('--contact-email', help='Optional override; prefer private config or env')
    args = parser.parse_args(argv)
    email = load_contact_email(args.volume, args.contact_email)
    with make_session(email) as session:
        run_ingestion(session, args.volume)


if __name__ == '__main__':
    main()
