import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, chmod, rm, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { gunzipSync } from 'node:zlib';

// No real production secrets or DB; imports only the backup process primitive.
process.env.AWS_ACCESS_KEY_ID = 'unit-test-only';
process.env.AWS_SECRET_ACCESS_KEY = 'unit-test-only';
process.env.AWS_S3_BUCKET = 'unit-test-bucket';
process.env.AWS_S3_REGION = 'auto';
process.env.BACKUP_DATABASE_URL = 'postgresql://invalid.test/example';
process.env.BACKUP_OPTIONS = '';

const { dumpToGzip } = await import('../dist/backup.js');

test('a failed pg_dump is not treated as successful gzip output', async () => {
  const work = await mkdtemp(join(tmpdir(), 'percy-dump-test-'));
  const priorPath = process.env.PATH;
  try {
    const executable = join(work, 'pg_dump');
    await writeFile(executable, '#!/bin/sh\nprintf "partial archive"\nexit 37\n');
    await chmod(executable, 0o700);
    process.env.PATH = work + ':' + priorPath;
    await assert.rejects(dumpToGzip(join(work, 'backup.tar.gz')), /pg_dump or compression failed/);
  } finally {
    process.env.PATH = priorPath;
    await rm(work, { recursive: true, force: true });
  }
});

test('successful pg_dump streams without shell interpolation and compresses', async () => {
  const work = await mkdtemp(join(tmpdir(), 'percy-dump-test-'));
  const priorPath = process.env.PATH;
  try {
    const executable = join(work, 'pg_dump');
    await writeFile(executable, '#!/bin/sh\nprintf "synthetic archive"\nexit 0\n');
    await chmod(executable, 0o700);
    process.env.PATH = work + ':' + priorPath;
    const dest = join(work, 'backup.tar.gz');
    await dumpToGzip(dest);
    assert.equal(gunzipSync(await readFile(dest)).toString('utf8'), 'synthetic archive');
  } finally {
    process.env.PATH = priorPath;
    await rm(work, { recursive: true, force: true });
  }
});
