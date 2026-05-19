/**
 * Workspace Scanner
 *
 * Periodically scans a directory for git repositories and runs
 * `runFullAnalysis` on each newly discovered repo. Controlled by
 * environment variables:
 *
 *   GITNEXUS_SCAN_INTERVAL  — scan interval in seconds (default: not set = disabled)
 *   GITNEXUS_SCAN_DIR       — directory to scan (default: /workspace)
 *   GITNEXUS_SCAN_DEPTH     — max depth for find (default: 4)
 *
 * When enabled, the scanner runs once on startup and repeats at
 * the configured interval. It only analyzes repos that are not yet
 * registered, so repeated cycles are cheap (already-up-to-date
 * short-circuits quickly).
 */

import path from 'path';
import { logger } from '../core/logger.js';
import { runFullAnalysis, type AnalyzeOptions } from '../core/run-analyze.js';

const DEFAULT_SCAN_DIR = '/workspace';
const DEFAULT_SCAN_DEPTH = 4;

function getEnvInt(name: string, defaultVal: number): number {
  const raw = process.env[name];
  if (!raw) return defaultVal;
  const n = parseInt(raw, 10);
  return Number.isFinite(n) && n > 0 ? n : defaultVal;
}

/**
 * Find all git repos under a root directory.
 */
async function discoverGitRepos(root: string, maxDepth: number): Promise<string[]> {
  const repos: string[] = [];
  const { execSync } = await import('child_process');
  try {
    const out = execSync(
      `find "${root}" -name ".git" -maxdepth ${maxDepth} -type d 2>/dev/null`,
      { encoding: 'utf-8', timeout: 30_000 },
    );
    for (const line of out.trim().split('\n').filter(Boolean)) {
      const repo = path.dirname(line);
      if (repo === root || !repo) continue;
      repos.push(repo);
    }
  } catch {
    // find produced no output or failed — no repos to scan
  }
  return repos;
}

/**
 * Start the workspace scanner. Returns a cleanup function to cancel
 * the timer. Does nothing if GITNEXUS_SCAN_INTERVAL is not set.
 */
export function startWorkspaceScanner(): () => void {
  const intervalRaw = process.env.GITNEXUS_SCAN_INTERVAL;
  if (!intervalRaw) {
    logger.info('GITNEXUS_SCAN_INTERVAL not set — workspace scanner disabled');
    return () => {};
  }

  const intervalMs = getEnvInt('GITNEXUS_SCAN_INTERVAL', 3600) * 1000;
  const scanDir = process.env.GITNEXUS_SCAN_DIR || DEFAULT_SCAN_DIR;
  const maxDepth = getEnvInt('GITNEXUS_SCAN_DEPTH', DEFAULT_SCAN_DEPTH);

  logger.info(
    { intervalMs, scanDir, maxDepth },
    'Starting workspace scanner',
  );

  let timer: ReturnType<typeof setInterval> | null = null;
  let running = false;

  async function scan(): Promise<void> {
    if (running) return; // don't overlap
    running = true;
    try {
      const repos = await discoverGitRepos(scanDir, maxDepth);
      if (repos.length === 0) {
        logger.debug('Workspace scanner: no git repos found');
        running = false;
        return;
      }

      let analyzedCount = 0;

      for (const repo of repos) {
        logger.info({ repo }, 'Workspace scanner: analyzing repo');
        try {
          await runFullAnalysis(
            repo,
            { force: false } as AnalyzeOptions,
            {
              onProgress: () => {},
              onLog: (msg: string) => logger.debug({ repo }, msg),
            },
          );
          analyzedCount++;
        } catch (err: any) {
          logger.error({ repo, err }, 'Workspace scanner: analyze failed');
        }
      }

      logger.info({ analyzedCount, total: repos.length }, 'Workspace scanner: cycle complete');
    } catch (err: any) {
      logger.error({ err }, 'Workspace scanner: cycle failed');
    } finally {
      running = false;
    }
  }

  // Run once on startup
  scan().catch((err) => logger.error({ err }, 'Workspace scanner: initial scan failed'));

  // Repeat on interval
  timer = setInterval(scan, intervalMs);
  timer.unref();

  return () => {
    if (timer) {
      clearInterval(timer);
      timer = null;
    }
  };
}
