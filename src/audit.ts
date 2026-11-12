// Read-only R2 audit. Uses the existing Railway service credentials in place.
// No bucket changes, object deletes, production DB queries or archive downloads.
import {
  GetBucketLifecycleConfigurationCommand, ListObjectsV2Command,
} from '@aws-sdk/client-s3';
import { backupS3Client, backupObjectKey } from './backup.js';
import { env } from './env.js';

async function audit() {
  const client = backupS3Client();
  try {
    const prefix = backupObjectKey(env.BACKUP_FILE_PREFIX + '-');
    const found: Array<{ bytes: number, modified: Date }> = [];
    let continuation: string | undefined;
    do {
      const result = await client.send(new ListObjectsV2Command({
        Bucket: env.AWS_S3_BUCKET,
        Prefix: prefix,
        ContinuationToken: continuation,
        MaxKeys: 1000,
      }));
      for (const object of result.Contents || []) {
        if (!object.Key?.endsWith('.tar.gz') || !object.LastModified) continue;
        found.push({ bytes: object.Size || 0, modified: object.LastModified });
      }
      continuation = result.IsTruncated ? result.NextContinuationToken : undefined;
    } while (continuation && found.length < 5000);

    found.sort((a, b) => b.modified.getTime() - a.modified.getTime());
    console.log('Backup archive objects matching configured prefix:', found.length);
    if (found.length) {
      console.log('Latest backup UTC:', found[0].modified.toISOString(), 'compressed bytes:', found[0].bytes);
      console.log('Oldest listed UTC:', found[found.length - 1].modified.toISOString());
      if (found.length > 1) {
        console.log('Previous backup UTC:', found[1].modified.toISOString(), 'compressed bytes:', found[1].bytes);
        const ratio = found[1].bytes ? Math.round(100 * found[0].bytes / found[1].bytes) : -1;
        console.log('Latest as percentage of preceding archive:', ratio);
        if (ratio > -1 && ratio < 50) console.warn('WARNING: latest backup compressed size dropped by more than 50%; investigate');
      }
      const hours = (Date.now() - found[0].modified.getTime()) / 3600000;
      if (hours > 36) console.warn('WARNING: most recent backup is older than 36 hours');
    } else {
      console.warn('WARNING: no matching archives found in R2');
    }

    try {
      const lifecycle = await client.send(new GetBucketLifecycleConfigurationCommand({
        Bucket: env.AWS_S3_BUCKET,
      }));
      const rules = (lifecycle.Rules || []).map(rule => ({
        enabled: rule.Status === 'Enabled',
        prefix: rule.Filter?.Prefix || rule.Prefix || rule.Filter?.And?.Prefix || '',
        days: rule.Expiration?.Days ?? null,
      }));
      const matches = rules.filter(rule => rule.enabled && prefix.startsWith(rule.prefix) && rule.days !== null);
      console.log('R2 lifecycle rules:', rules.length, 'enabled expiry rules matching backup prefix:', matches.length);
      for (const rule of matches) console.log('Matching expiration age (days):', rule.days);
      if (!matches.length) console.warn('WARNING: no verified expiration rule covers the configured backup prefix');
      if (!matches.some(rule => rule.days === 35)) {
        console.warn('WARNING: the previously discussed 35-day retention is NOT confirmed by the matching lifecycle rules');
      }
    } catch (e) {
      const name = e instanceof Error ? e.name : 'Unknown';
      console.warn('R2 lifecycle inspection unavailable:', name);
      console.warn('WARNING: retention remains unverified — R2 permissions or lifecycle configuration need review');
    }

    if (!found.length) process.exitCode = 2;
    console.log('Audit is read-only. It does NOT prove a backup can restore or that erased data stays beyond use.');
  } finally {
    client.destroy();
  }
}

audit().catch((err) => {
  // Never serialize SDK errors: endpoint, metadata or auth context can be sensitive.
  console.error('Read-only R2 audit failed:', err instanceof Error ? err.name : 'Unknown');
  process.exitCode = 1;
});
