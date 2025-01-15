import pino from "pino";
import { LRUCache } from "lru-cache";
import { Pool, ResultSetHeader, RowDataPacket } from "mysql2/promise";

export class BatchProcessor {
  private batchQueues = {
    messages: new Map<string, any[]>(),
    status_updates: new Map<string, any[]>(),
    status_viewers: new Map<string, any[]>(),
    contacts: new Map<string, any[]>(),
    chats: new Map<string, any[]>(),
    groups_metadata: new Map<string, any[]>()
  };
  private processing = false;
  private readonly BATCH_SIZE = 100;
  private readonly PROCESS_INTERVAL = 1000;

  constructor(private pool: Pool, private log: pino.Logger) {
    this.startProcessor();
  }

  public queueItem(tableName: string, data: Record<string, any>) {
    if (!this.batchQueues[tableName]) return;
    const key = this.getBatchKey(tableName, data);
    if (!this.batchQueues[tableName].has(key)) {
      this.batchQueues[tableName].set(key, []);
    }
    this.batchQueues[tableName].get(key)!.push(data);
  }

  private getBatchKey(tableName: string, data: Record<string, any>): string {
    switch (tableName) {
      case "status_viewers":
        return `${data.status_id}_${data.viewer_jid}`;
      case "status_updates":
        return data.status_id;
      default:
        return data.id || data.jid || JSON.stringify(data);
    }
  }

  private async processBatch() {
    if (this.processing) return;
    this.processing = true;

    const conn = await this.pool.getConnection();
    try {
      await conn.beginTransaction();

      for (const [tableName, queue] of Object.entries(this.batchQueues)) {
        if (queue.size === 0) continue;

        const batch = Array.from(queue.values())
          .flat()
          .slice(0, this.BATCH_SIZE);

        if (batch.length === 0) continue;

        await this.executeBatchUpsert(conn, tableName, batch);

        for (const item of batch) {
          const key = this.getBatchKey(tableName, item);
          queue.delete(key);
        }
      }

      await conn.commit();
    } catch (error) {
      await conn.rollback();
      this.log.error({ error }, "Batch processing failed");
    } finally {
      conn.release();
      this.processing = false;
    }
  }

  private async executeBatchUpsert(
    conn: any,
    tableName: string,
    batch: Record<string, any>[]
  ) {
    const columns = Object.keys(batch[0]);
    const placeholders = batch
      .map(() => `(${columns.map(() => "?").join(",")})`)
      .join(",");

    const updateClauses = columns
      .map((col) => `${col} = VALUES(${col})`)
      .join(",");

    const query = `
      INSERT INTO ${tableName} (${columns.join(",")})
      VALUES ${placeholders}
      ON DUPLICATE KEY UPDATE ${updateClauses}
    `;

    const values = batch.flatMap((item) => columns.map((col) => item[col]));
    await conn.query(query, values);
  }

  private startProcessor() {
    setInterval(() => this.processBatch(), this.PROCESS_INTERVAL);
  }
}

export class DbHelpers {
  constructor(
    private pool: Pool,
    private log: pino.Logger,
    private cache: LRUCache<string, any>
  ) {}

  async getFromCacheOrDb<T>(
    cacheKey: string,
    query: string,
    params: any[],
    transform: (row: RowDataPacket) => T
  ): Promise<T | null> {
    if (this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey) as T;
    }

    try {
      const [rows] = await this.pool.query<RowDataPacket[]>(query, params);
      if (rows.length === 0) return null;

      const result = transform(rows[0]);
      this.cache.set(cacheKey, result);
      return result;
    } catch (error) {
      this.log.error({ error, query }, "Database query failed");
      return null;
    }
  }

  async batchQuery(
    query: string,
    batchParams: any[][],
    batchSize: number = 500
  ): Promise<Array<RowDataPacket[] | ResultSetHeader>> {
    const results: Array<RowDataPacket[] | ResultSetHeader> = [];
    for (let i = 0; i < batchParams.length; i += batchSize) {
      const batch = batchParams.slice(i, i + batchSize);
      const [result] = await this.pool.query<RowDataPacket[] | ResultSetHeader>(
        query,
        [batch]
      );
      results.push(result);
    }
    return results;
  }
}
