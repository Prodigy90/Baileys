import { LRUCache } from 'lru-cache'
import { type Pool, type RowDataPacket } from 'mysql2/promise'
import pino from 'pino'

interface GroupRow extends RowDataPacket {
	jid: string
	metadata: Record<string, unknown>
}

interface ContactRow extends RowDataPacket {
	jid: string
	contact: Record<string, unknown>
}

interface UserRow extends RowDataPacket {
	username: string
	jid: string
}

export class CacheWarmer {
	private warmupInterval: NodeJS.Timeout | null = null
	private isWarming = false

	constructor(
		private pool: Pool,
		private cache: LRUCache<string, Record<string, unknown>>,
		private instanceId: string,
		private logger: pino.Logger,
		private warmupIntervalMs: number = 1000 * 60 * 30
	) {}

	async start() {
		this.stop()

		await this.warmCache()
		this.warmupInterval = setInterval(() => {
			this.warmCache().catch(err => this.logger.error({ err }, 'Cache warming interval failed'))
		}, this.warmupIntervalMs)

		this.logger.info('Cache warming started')
	}

	stop() {
		if (this.warmupInterval) {
			clearInterval(this.warmupInterval)
			this.warmupInterval = null
			this.logger.info('Cache warming stopped')
		}
	}

	private async warmCache() {
		if (this.isWarming) {
			this.logger.debug('Cache warming already in progress')
			return
		}

		this.isWarming = true
		try {
			await Promise.all([this.warmGroupMetadata(), this.warmContacts(), this.warmUserData()])
			this.logger.info('Cache warming completed successfully')
		} catch (error) {
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

		const [rows] = await this.pool.query<GroupRow[]>(query, [this.instanceId])
		for (const row of rows) {
			const cacheKey = `group_${this.instanceId}_${row.jid}`
			this.cache.set(cacheKey, row.metadata, {
				ttl: 1000 * 60 * 15
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

		const [rows] = await this.pool.query<ContactRow[]>(query, [this.instanceId])
		for (const row of rows) {
			const cacheKey = `contact_${this.instanceId}_${row.jid}`
			this.cache.set(cacheKey, row.contact, {
				ttl: 1000 * 60 * 30
			})
		}
	}

	private async warmUserData() {
		const query = `
      SELECT username, jid
      FROM users
      WHERE instance_id = ?
    `

		const [rows] = await this.pool.query<UserRow[]>(query, [this.instanceId])
		if (rows && rows.length > 0) {
			const cacheKey = `${this.instanceId}_user_cache`
			this.cache.set(cacheKey, rows[0], {
				ttl: 1000 * 60 * 30
			})
		}
	}
}
