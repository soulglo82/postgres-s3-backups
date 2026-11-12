import { CronJob } from "cron";
import { backup } from "./backup.js";
import { auditR2 } from "./audit.js";
import { env } from "./env.js";

console.log("NodeJS Version: " + process.version);

const tryBackup = async () => {
  try {
    await backup();
    // Audit object inventory, size changes and lifecycle while the normal
    // Railway backup service already has its existing R2 credentials.
    // The audit is read-only; an unsupported ListBucket permission is warned
    // about, but does not discard a successfully verified backup.
    try {
      await auditR2();
    } catch (error) {
      console.warn('Read-only R2 inventory unavailable:', error instanceof Error ? error.name : 'Unknown');
    }
  } catch (error) {
    console.error("Error while running backup: ", error);
    process.exit(1);
  }
}

if (env.RUN_ON_STARTUP || env.SINGLE_SHOT_MODE) {
  console.log("Running on start backup...");

  await tryBackup();

  if (env.SINGLE_SHOT_MODE) {
    console.log("Database backup complete, exiting...");
    process.exit(0);
  }
}

const job = new CronJob(env.BACKUP_CRON_SCHEDULE, async () => {
  await tryBackup();
});

job.start();

console.log("Backup cron scheduled...");