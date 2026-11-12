import { spawn, execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { createReadStream, createWriteStream } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { pipeline } from 'node:stream/promises';
import { createGzip, createGunzip } from 'node:zlib';
import { createHash } from 'node:crypto';
import path from 'node:path';
import os from 'node:os';
import {
  S3Client, S3ClientConfig, HeadObjectCommand, GetObjectCommand,
} from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import { env } from './env.js';

const execFileAsync = promisify(execFile);
const REQUIRED_TABLES = ['families', 'children', 'point_events', 'rewards', 'uploads'];

export function backupObjectKey(filename: string): string {
  return env.BUCKET_SUBFOLDER ? env.BUCKET_SUBFOLDER.replace(/\/$/, '') + '/' + filename : filename;
}

export function backupS3Client(): S3Client {
  const config: S3ClientConfig = {
    region: env.AWS_S3_REGION,
    forcePathStyle: env.AWS_S3_FORCE_PATH_STYLE,
  };
  if (env.AWS_S3_ENDPOINT) config.endpoint = env.AWS_S3_ENDPOINT;
  return new S3Client(config);
}

// Do not interpolate DATABASE_URL into a shell command. In particular,
// "pg_dump | gzip" previously reported gzip's zero exit status even if
// pg_dump had failed. Both process exit statuses are now explicitly checked.
export async function dumpToGzip(destination: string): Promise<void> {
  const extra = env.BACKUP_OPTIONS.trim()
    ? env.BACKUP_OPTIONS.trim().split(/\s+/).filter(Boolean)
    : [];
  if (extra.some(x => /["'\\]/.test(x))) {
    throw new Error('BACKUP_OPTIONS must be individual whitespace-separated arguments without shell quoting');
  }
  const dump = spawn('pg_dump', [
    '--dbname', env.BACKUP_DATABASE_URL, '--format=tar', ...extra,
  ], { stdio: ['ignore', 'pipe', 'pipe'] });
  let failureText = '';
  dump.stderr.setEncoding('utf8');
  dump.stderr.on('data', (text: string) => {
    failureText = (failureText + text).slice(-4000);
  });
  const dumpExit = new Promise<number>((resolve, reject) => {
    dump.once('error', reject);
    dump.once('close', (code) => resolve(code ?? -1));
  });
  const streamed = pipeline(dump.stdout, createGzip(), createWriteStream(destination, {
    mode: 0o600,
  }));
  const results = await Promise.allSettled([dumpExit, streamed]);
  if (results[0].status === 'rejected' || results[1].status === 'rejected' ||
      results[0].value !== 0) {
    // Do not include stderr or the pg_dump command here: either may contain
    // a credential embedded in the connection string.
    throw new Error('pg_dump or compression failed; refusing to upload backup');
  }
  if (failureText.trim()) console.warn('pg_dump emitted warnings (details withheld)');
}

// pg_restore --list validates the archive's table of contents, not a full
// restore. A separate isolated restore drill is still mandatory.
async function verifyArchive(gzipFile: string, tarFile: string): Promise<void> {
  await pipeline(createReadStream(gzipFile), createGunzip(), createWriteStream(tarFile, {
    mode: 0o600,
  }));
  const { stdout } = await execFileAsync('pg_restore', ['--list', tarFile], {
    maxBuffer: 8 * 1024 * 1024,
  });
  for (const name of REQUIRED_TABLES) {
    if (!stdout.includes('TABLE DATA public ' + name)) {
      throw new Error('Archive validation failed: missing required table ' + name);
    }
  }
}

async function sha256File(filename: string): Promise<{ hash: string, size: number }> {
  const hash = createHash('sha256');
  let size = 0;
  for await (const data of createReadStream(filename)) {
    const chunk = data as Buffer;
    size += chunk.length;
    hash.update(chunk);
  }
  return { hash: hash.digest('hex'), size };
}

async function verifyRemote(client: S3Client, key: string, expected: {
  hash: string, size: number,
}): Promise<void> {
  const head = await client.send(new HeadObjectCommand({
    Bucket: env.AWS_S3_BUCKET, Key: key,
  }));
  if (head.ContentLength !== expected.size) {
    throw new Error('R2 object size does not match the original backup');
  }
  const object = await client.send(new GetObjectCommand({
    Bucket: env.AWS_S3_BUCKET, Key: key,
  }));
  if (!object.Body) throw new Error('R2 object download did not return a body');
  const hash = createHash('sha256');
  let size = 0;
  for await (const part of object.Body as unknown as AsyncIterable<Uint8Array>) {
    hash.update(part);
    size += part.length;
  }
  if (size !== expected.size || hash.digest('hex') !== expected.hash) {
    throw new Error('R2 backup download checksum mismatch');
  }
}

export const backup = async () => {
  const work = await mkdtemp(path.join(os.tmpdir(), 'percy-pg-backup-'));
  const filename = `${env.BACKUP_FILE_PREFIX}-${new Date().toISOString().replace(/[:.]+/g, '-')}.tar.gz`;
  const gzipFile = path.join(work, 'archive.tar.gz');
  const tarFile = path.join(work, 'archive.tar');
  const key = backupObjectKey(filename);
  const client = backupS3Client();
  try {
    console.log('Creating validated database backup...');
    await dumpToGzip(gzipFile);
    await verifyArchive(gzipFile, tarFile);
    const digest = await sha256File(gzipFile);
    if (digest.size < 1024) throw new Error('Backup archive suspiciously small');
    console.log('Archive validated using pg_restore catalog; compressed bytes:', digest.size);
    await new Upload({
      client, params: {
        Bucket: env.AWS_S3_BUCKET,
        Key: key,
        Body: createReadStream(gzipFile),
      },
    }).done();
    if (env.BACKUP_VERIFY_READBACK) {
      await verifyRemote(client, key, digest);
      console.log('R2 readback SHA-256 and byte length match source');
    } else {
      console.warn('R2 readback verification is disabled; upload is not independently checked');
    }
    console.log('Database backup uploaded and validated');
  } finally {
    client.destroy();
    await rm(work, { recursive: true, force: true });
  }
};
