import pino from 'pino'
import { LRUCache } from 'lru-cache'
import { type Pool, type ResultSetHeader, type RowDataPacket } from 'mysql2/promise'

interface BatchItem {
	priority: number
	tableName: string
	data: Record<string, any>
}

class PriorityQueue<T> {
	private items: T[] = []

	enqueue(item: T & { priority: number }): void {
		let added = false
		for (let i = 0; i < this.items.length; i++) {
			if ((this.items[i] as any).priority > item.priority) {
				this.items.splice(i, 0, item)
				added = true
				break
			}
		}

		if (!added) {
			this.items.push(item)
		}
	}

	dequeue(): T | undefined {
		return this.items.shift()
	}

	isEmpty(): boolean {
		return this.items.length === 0
	}

	size(): number {
		return this.items.length
	}
}

export class BatchProcessor {
	private queues: Map<string, PriorityQueue<BatchItem>> = new Map()
	private processing = false
	private readonly BATCH_SIZE = 100
	private readonly PROCESS_INTERVAL = 5000

	constructor(
		private pool: Pool,
		private log: pino.Logger
	) {
		this.startProcessor()
	}

	public queueItem(tableName: string, data: Record<string, any>, priority = 1): void {
		if (!this.queues.has(tableName)) {
			this.queues.set(tableName, new PriorityQueue<BatchItem>())
		}

		this.queues.get(tableName)!.enqueue({
			priority,
			tableName,
			data
		})

		// Track queue size for debugging if needed
		// const currentQueueSize = this.queues.get(tableName)?.size() || 0;
	}

	private async processBatch(): Promise<void> {
		if (this.processing) {
			return
		}

		this.processing = true
		let retries = 3

		try {
			while (retries > 0) {
				const conn = await this.pool.getConnection()

				try {
					await conn.beginTransaction()

					for (const [tableName, queue] of this.queues.entries()) {
						const batch: BatchItem[] = []
						const failedItems: BatchItem[] = []

						while (!queue.isEmpty() && batch.length < this.BATCH_SIZE) {
							const item = queue.dequeue()
							if (item) {
								batch.push(item)
							}
						}

						if (batch.length > 0) {
							try {
								await this.executeBatchUpsert(
									conn,
									tableName,
									batch.map(b => b.data)
								)
							} catch (error) {
								batch.forEach(item => failedItems.push(item))
								throw error
							}
						}

						failedItems.forEach(item => queue.enqueue(item))
					}

					await conn.commit()
					break
				} catch (error) {
					await conn.rollback()
					retries--

					this.log.error(
						{
							error,
							retriesLeft: retries
						},
						'Batch processing failed, retrying...'
					)

					if (retries === 0) {
						throw error
					}

					await new Promise(resolve => setTimeout(resolve, 1000))
				} finally {
					conn.release()
				}
			}
		} catch (error) {
			this.log.error(
				{
					error
				},
				'Batch processing failed after all retries'
			)
			throw error
		} finally {
			this.processing = false
		}
	}

	/**
	 * Get a unique key for a batch item
	 * This is used for deduplication but currently not actively used
	 * Keeping for future reference
	 */
	/*
  private getBatchKey(tableName: string, data: Record<string, any>): string {
    switch (tableName) {
      case "status_viewers":
        return `${data.status_id}_${data.viewer_jid}`;
      case "status_updates":
        return data.status_id;
      case "status_view_counts":
        return data.status_id;
      default:
        return data.id || data.jid || data;
    }
  }
  */

	private async executeBatchUpsert(conn: any, tableName: string, batch: Record<string, any>[]) {
		switch (tableName) {
			case 'status_updates':
				if (batch[0]?.view_count_increment) {
					const statusIds = batch.map(item => item.id)
					const increments = batch.map(item => item.view_count_increment)

					const query = `
            UPDATE status_updates
            SET view_count = view_count + CASE
              ${statusIds.map(() => 'WHEN status_id = ? THEN ?').join(' ')}
            END
            WHERE status_id IN (${statusIds.map(() => '?').join(',')})
          `

					const values = [...statusIds.flatMap((id, i) => [id, increments[i]]), ...statusIds]

					await conn.query(query, values)
					return
				}

				try {
					const columns = ['instance_id', 'status_id', 'status_message', 'post_date', 'message_type']

					const rowPlaceholders = batch.map(() => '(?, ?, ?, ?, ?)').join(', ')

					const updatePlaceholders = columns.map(key => `\`${key}\` = VALUES(\`${key}\`)`).join(', ')

					const query = `
            INSERT INTO status_updates (${columns.join(', ')})
            VALUES ${rowPlaceholders}
            ON DUPLICATE KEY UPDATE ${updatePlaceholders}
          `

					const values = batch.flatMap(item => [
						item.instance_id,
						item.status_id,
						JSON.stringify(item.status_message),
						item.post_date,
						item.message_type
					])

					await conn.query(query, values)
				} catch (error) {
					this.log.error({ error }, 'Failed to execute status_updates batch upsert')
					throw error
				}

				return

			case 'messages':
				const query = `
        INSERT INTO messages (instance_id, message_id, message_data, post_date)
        VALUES ${batch.map(() => '(?, ?, ?, ?)').join(',')}
        ON DUPLICATE KEY UPDATE
          instance_id = VALUES(instance_id),
          message_id = VALUES(message_id),
          message_data = VALUES(message_data),
          post_date = VALUES(post_date)
      `

				const values = batch.flatMap(item => {
					const message_data =
						typeof item.message_data === 'object' ? JSON.stringify(item.message_data) : item.message_data

					return [item.instance_id, item.message_id, message_data, item.post_date]
				})

				try {
					await conn.query(query, values)
				} catch (error) {
					throw error
				}

				return

			case 'contacts':
				const contactQuery = `
        INSERT INTO contacts (instance_id, jid, contact)
        VALUES ${batch.map(() => '(?, ?, ?)').join(',')}
        ON DUPLICATE KEY UPDATE
          contact = JSON_SET(
            VALUES(contact),
            '$.name', IFNULL(
              JSON_UNQUOTE(JSON_EXTRACT(contacts.contact, '$.name')),
              JSON_UNQUOTE(JSON_EXTRACT(VALUES(contact), '$.name'))
            ),
            '$.notify', IFNULL(
              JSON_UNQUOTE(JSON_EXTRACT(contacts.contact, '$.notify')),
              JSON_UNQUOTE(JSON_EXTRACT(VALUES(contact), '$.notify'))
            )
          )
      `

				const contactValues = batch.flatMap(item => [item.instance_id, item.jid, JSON.stringify(item.contact)])

				await conn.query(contactQuery, contactValues)
				return

			case 'chats':
				const chatQuery = `
        INSERT INTO chats (instance_id, jid, chat)
        VALUES ${batch.map(() => '(?, ?, ?)').join(',')}
        ON DUPLICATE KEY UPDATE
         chat = VALUES(chat)
      `

				const chatValues = batch.flatMap(item => [item.instance_id, item.jid, JSON.stringify(item.chat)])

				await conn.query(chatQuery, chatValues)
				return

			case 'groups_metadata':
				if (batch[0]?.metadata) {
					const groupQuery = `
          INSERT INTO groups_metadata
            (instance_id, jid, subject, is_admin, participating, group_index, admin_index, metadata)
          VALUES ${batch.map(() => '(?, ?, ?, ?, ?, ?, ?, ?)').join(',')}
          ON DUPLICATE KEY UPDATE
            subject = VALUES(subject),
            is_admin = VALUES(is_admin),
            participating = VALUES(participating),
            group_index = VALUES(group_index),
            admin_index = VALUES(admin_index),
            metadata = VALUES(metadata)
        `

					const groupValues = batch.flatMap(item => [
						item.instance_id,
						item.jid,
						item.subject || '',
						item.is_admin || false,
						item.participating !== false,
						item.group_index,
						item.admin_index || 0,
						JSON.stringify(item.metadata)
					])

					await conn.query(groupQuery, groupValues)
					return
				}

				return

			case 'status_viewers':
				const viewerQuery = `
        INSERT INTO status_viewers
          (instance_id, status_id, viewer_jid, view_date)
        VALUES ${batch.map(() => '(?, ?, ?, ?)').join(',')}
        ON DUPLICATE KEY UPDATE
          view_date = VALUES(view_date)
      `

				const viewerValues = batch.flatMap(item => [item.instance_id, item.status_id, item.viewer_jid, item.view_date])

				await conn.query(viewerQuery, viewerValues)
				return

			case 'status_view_counts':
				if (batch[0]?.total_views_increment) {
					// Handle batch increment of view counts
					const statusIds = batch.map(item => item.status_id)
					const increments = batch.map(item => item.total_views_increment)
					const lastUpdates = batch.map(item => item.last_updated)

					const incrementQuery = `
            UPDATE status_view_counts
            SET total_views = total_views + CASE
              ${statusIds.map(() => 'WHEN status_id = ? THEN ?').join(' ')}
            END,
            last_updated = CASE
              ${statusIds.map(() => 'WHEN status_id = ? THEN ?').join(' ')}
            END
            WHERE status_id IN (${statusIds.map(() => '?').join(',')})
          `

					const incrementValues = [
						...statusIds.flatMap((id, i) => [id, increments[i]]),
						...statusIds.flatMap((id, i) => [id, lastUpdates[i]]),
						...statusIds
					]

					await conn.query(incrementQuery, incrementValues)
				} else {
					// Handle insert/update of view counts
					const viewCountQuery = `
          INSERT INTO status_view_counts
            (instance_id, status_id, media_type, total_views, last_updated)
          VALUES ${batch.map(() => '(?, ?, ?, ?, ?)').join(',')}
          ON DUPLICATE KEY UPDATE
            media_type = VALUES(media_type),
            total_views = VALUES(total_views),
            last_updated = VALUES(last_updated)
          `

					const viewCountValues = batch.flatMap(item => [
						item.instance_id,
						item.status_id,
						item.media_type,
						item.total_views || 0,
						item.last_updated
					])

					await conn.query(viewCountQuery, viewCountValues)
				}

				return
		}
	}

	private startProcessor() {
		setInterval(() => this.processBatch(), this.PROCESS_INTERVAL)
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
			return this.cache.get(cacheKey) as T
		}

		try {
			const [rows] = await this.pool.query<RowDataPacket[]>(query, params)
			if (rows.length === 0) {
				return null
			}

			const result = transform(rows[0]!)
			this.cache.set(cacheKey, result)
			return result
		} catch (error) {
			this.log.error({ error, query }, 'Database query failed')
			return null
		}
	}

	async checkExists(cacheKey: string, query: string, params: any[]): Promise<boolean> {
		return (await this.getFromCacheOrDb<boolean>(cacheKey, query, params, row => !!row)) ?? false
	}

	async batchQuery(
		query: string,
		batchParams: any[][],
		batchSize = 500
	): Promise<Array<RowDataPacket[] | ResultSetHeader>> {
		const results: Array<RowDataPacket[] | ResultSetHeader> = []
		for (let i = 0; i < batchParams.length; i += batchSize) {
			const batch = batchParams.slice(i, i + batchSize)
			const [result] = await this.pool.query<RowDataPacket[] | ResultSetHeader>(query, [batch])
			results.push(result)
		}

		return results
	}
}
