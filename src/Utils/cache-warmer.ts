import pino from 'pino'
import { LRUCache } from 'lru-cache'
import { Pool } from 'mysql2/promise'

export class CacheWarmer {
	private warmupInterval: NodeJS.Timeout | null = null
	private isWarming = false

	constructor(
    private pool: Pool,
    private cache: LRUCache<string, any>,
    private instance_id: string,
    private logger: pino.Logger,
    private warmupIntervalMs: number = 1000 * 60 * 30
	) {}

	async start() {
		this.stop()

		await this.warmCache()
		this.warmupInterval = setInterval(() => {
			this.warmCache().catch((err) => this.logger.error({ err }, 'Cache warming interval failed')
			)
		}, this.warmupIntervalMs)

		this.logger.info('Cache warming started')
	}

	stop() {
		if(this.warmupInterval) {
			clearInterval(this.warmupInterval)
			this.warmupInterval = null
			this.logger.info('Cache warming stopped')
		}
	}

	private async warmCache() {
		if(this.isWarming) {
			this.logger.debug('Cache warming already in progress')
			return
		}

		this.isWarming = true
		try {
			await Promise.all([
				this.warmGroupMetadata(),
				this.warmContacts(),
				this.warmUserData(),
			])
			this.logger.info('Cache warming completed successfully')
		} catch(error) {
			this.logger.error({ error }, 'Cache warming failed')
		} finally {
			this.isWarming = false
		}
	}

	private async warmGroupMetadata() {
		const query = `
      SELECT jid, metadata 
      FROM groups_metadata 
      WHERE instance_id = ? 
        AND participating = 1
      ORDER BY group_index ASC
    `

		const [rows] = await this.pool.query(query, [this.instance_id])
		for(const row of rows as any[]) {
			const cacheKey = `group_${this.instance_id}_${row.jid}`
			this.cache.set(cacheKey, row.metadata, {
				ttl: 1000 * 60 * 15,
			})
		}
	}

	private async warmContacts() {
		const query = `
      SELECT jid, contact 
      FROM contacts 
      WHERE instance_id = ?
      ORDER BY JSON_EXTRACT(contact, '$.name') ASC
      LIMIT 1000
    `

		const [rows] = await this.pool.query(query, [this.instance_id])
		for(const row of rows as any[]) {
			const cacheKey = `contact_${this.instance_id}_${row.jid}`
			this.cache.set(cacheKey, row.contact, {
				ttl: 1000 * 60 * 30,
			})
		}
	}

	private async warmUserData() {
		const query = `
      SELECT username, jid 
      FROM users 
      WHERE instance_id = ?
    `

		const [rows] = await this.pool.query(query, [this.instance_id])
		if(rows && (rows as any[]).length > 0) {
			const cacheKey = `${this.instance_id}_user_cache`
			this.cache.set(cacheKey, rows[0], {
				ttl: 1000 * 60 * 30,
			})
		}
	}
}