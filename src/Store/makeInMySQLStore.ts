import { type Pool } from 'mysql2/promise'
import pino from 'pino'
import { proto } from '../../WAProto'
import type makeMDSocket from '../Socket'
import {
	type BaileysEventEmitter,
	type Chat,
	type ConnectionState,
	type Contact,
	type GroupMetadata,
	type GroupMetadataResult,
	type GroupMetadataRow
} from '../Types'
import { OptimizedMySQLStore } from './optimized-mysql-store'

type WASocket = ReturnType<typeof makeMDSocket>

interface makeMySQLStoreFunc {
	state: ConnectionState | null
	bind: (ev: BaileysEventEmitter) => Promise<void>
	loadMessage: (id: string) => Promise<proto.IWebMessageInfo | undefined>
	loadAllGroupsMetadata: () => Promise<GroupMetadata[]>
	customQuery: (query: string, params?: unknown[]) => Promise<unknown>
	getAllChats: () => Promise<Chat[]>
	getAllContacts: () => Promise<Contact[]>
	getAllSavedContacts: () => Promise<Contact[]>
	fetchAllGroupsMetadata: (sock: WASocket | undefined) => Promise<GroupMetadataResult>
	getChatById: (jid: string) => Promise<Chat | undefined>
	getContactById: (jid: string) => Promise<Contact | undefined>
	getGroupByJid: (jid: string) => Promise<GroupMetadataRow | null>
	removeAllData: () => Promise<void>
	getRecentStatusUpdates: (options?: { limit?: number; offset?: number }) => Promise<proto.IWebMessageInfo[]>
	fetchGroupMetadata: (jid: string, sock: WASocket | undefined) => Promise<GroupMetadata | null>
	clearGroupsData: () => Promise<void>
	toJSON: () => Promise<Record<string, unknown>>
	fromJSON: (json: {
		chats: Chat[]
		contacts: Contact[]
		messages: { [id: string]: any[] }
	}) => Promise<{ totalChatsAffected: number; totalContactsAffected: number }>
	storeUserData: (jid: string, username: string | null, lid?: string | null) => Promise<void>
	getUserLid: () => Promise<string | null>
	storeStatusUpdate: (message: proto.IWebMessageInfo) => Promise<boolean>
	cleanupStatusData: (viewerRetentionDays?: number, countRetentionDays?: number) => Promise<void>
}

export function makeMySQLStore(
	instanceId: string,
	pool: Pool,
	skippedGroups: string[],
	logger?: pino.Logger
): makeMySQLStoreFunc {
	if (!pool) {
		throw new Error('No MySQL connection pool provided')
	}

	const log = logger || pino({ level: 'info' })
	const store = new OptimizedMySQLStore(pool, log, instanceId, skippedGroups)

	const checkAndUpdateSchema = async () => {
		// Check if lid column exists in users table
		try {
			const [columns] = await pool.query(
				`SELECT COLUMN_NAME
				FROM INFORMATION_SCHEMA.COLUMNS
				WHERE TABLE_SCHEMA = DATABASE()
				AND TABLE_NAME = 'users'
				AND COLUMN_NAME = 'lid'`
			)

			if (Array.isArray(columns) && columns.length === 0) {
				log.info('Adding lid column to users table')
				await pool.query(
					`ALTER TABLE users
					ADD COLUMN lid VARCHAR(255) NULL`
				)
				log.info('Successfully added lid column to users table')
			}
		} catch (error) {
			log.error({ error }, 'Failed to check or update users table schema')
		}
	}

	const createTables = async () => {
		const schema = [
			`CREATE TABLE IF NOT EXISTS status_updates (
        id INT AUTO_INCREMENT PRIMARY KEY,
        instance_id VARCHAR(255) NOT NULL,
        message_type VARCHAR(50) NOT NULL,
        status_id VARCHAR(255) NOT NULL,
        post_date DATETIME NOT NULL,
        view_count INT DEFAULT 0,
        status_message JSON,
        INDEX idx_instance_date (instance_id, post_date),
        INDEX idx_post_date (post_date),
        UNIQUE(instance_id, status_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,

			`CREATE TABLE IF NOT EXISTS status_view_counts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        instance_id VARCHAR(255) NOT NULL,
        status_id VARCHAR(255) NOT NULL,
        media_type VARCHAR(50) NOT NULL,
        total_views INT DEFAULT 0,
        last_updated DATETIME NOT NULL,
        INDEX idx_instance_status (instance_id, status_id),
        INDEX idx_cleanup (last_updated),
        UNIQUE(instance_id, status_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,

			`CREATE TABLE IF NOT EXISTS messages (
        id INT AUTO_INCREMENT PRIMARY KEY,
        instance_id VARCHAR(255) NOT NULL,
        message_id VARCHAR(255) NOT NULL,
        post_date DATETIME NOT NULL,
        message_data JSON,
        INDEX idx_instance_date (instance_id, post_date),
        UNIQUE(instance_id, message_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,

			// Status Viewers Table
			`CREATE TABLE IF NOT EXISTS status_viewers (
        id INT AUTO_INCREMENT PRIMARY KEY,
        instance_id VARCHAR(255) NOT NULL,
        status_id VARCHAR(255) NOT NULL,
        viewer_jid VARCHAR(255) NOT NULL,
        view_date DATETIME NOT NULL,
        INDEX idx_instance_status (instance_id, status_id),
        UNIQUE(instance_id, status_id, viewer_jid)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,

			// Contacts Table
			`CREATE TABLE IF NOT EXISTS contacts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        instance_id VARCHAR(255) NOT NULL,
        jid VARCHAR(255) NOT NULL,
        contact JSON,
        INDEX idx_instance_jid (instance_id, jid),
        UNIQUE(instance_id, jid)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,

			// Chats Table
			`CREATE TABLE IF NOT EXISTS chats (
        id INT AUTO_INCREMENT PRIMARY KEY,
        instance_id VARCHAR(255) NOT NULL,
        jid VARCHAR(255) NOT NULL,
        chat JSON,
        conversation_timestamp TIMESTAMP GENERATED ALWAYS AS (
            IF(
                COALESCE(
                    IF(
                        JSON_TYPE(JSON_EXTRACT(chat, '$.conversationTimestamp')) = 'INTEGER',
                        JSON_EXTRACT(chat, '$.conversationTimestamp'),
                        JSON_EXTRACT(chat, '$.conversationTimestamp.low')
                    ),
                    0
                ) > 0,
                FROM_UNIXTIME(
                    IF(
                        JSON_TYPE(JSON_EXTRACT(chat, '$.conversationTimestamp')) = 'INTEGER',
                        JSON_EXTRACT(chat, '$.conversationTimestamp'),
                        JSON_EXTRACT(chat, '$.conversationTimestamp.low')
                    )
                ),
                NULL
            )
        ) STORED,
        INDEX idx_instance_timestamp (instance_id, conversation_timestamp DESC),
        UNIQUE(instance_id, jid)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,

			// Groups Metadata Table
			`CREATE TABLE IF NOT EXISTS groups_metadata (
        id INT AUTO_INCREMENT PRIMARY KEY,
        participating BOOLEAN DEFAULT TRUE,
        instance_id VARCHAR(255) NOT NULL,
        is_admin BOOLEAN DEFAULT FALSE,
        subject VARCHAR(255) NOT NULL,
        jid VARCHAR(255) NOT NULL,
        group_index INT DEFAULT 0,
        admin_index INT DEFAULT 0,
        metadata JSON,
        INDEX idx_instance_participating (instance_id, participating),
        INDEX idx_instance_admin (instance_id, is_admin),
        UNIQUE(instance_id, jid)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,

			// Groups Status Table
			`CREATE TABLE IF NOT EXISTS groups_status (
        id INT AUTO_INCREMENT PRIMARY KEY,
        instance_id VARCHAR(255) NOT NULL,
        status BOOLEAN DEFAULT FALSE,
        group_index INT DEFAULT 0,
        admin_index INT DEFAULT 0,
        INDEX idx_instance_status (instance_id, status),
        UNIQUE(instance_id, group_index, admin_index)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,

			// Users Table
			`CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        instance_id VARCHAR(255) NOT NULL,
        username VARCHAR(255) NULL,
        jid VARCHAR(255) NULL,
        lid VARCHAR(255) NULL,
        INDEX idx_instance_jid (instance_id, jid),
        UNIQUE(instance_id, jid, username)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`
		]

		for (const query of schema) {
			try {
				await pool.query(query)
				log.info(`Schema operation executed: ${query.slice(0, 50)}...`)
			} catch (error) {
				log.error({ error, query }, 'Failed to execute schema operation')
			}
		}
	}

	// First create tables, then check and update schema if needed
	createTables()
		.then(() => checkAndUpdateSchema())
		.catch(err => log.error({ err }, 'Failed to create or update database tables'))

	return {
		state: store.state,
		bind: store.bind.bind(store),
		toJSON: store.toJSON.bind(store),
		fromJSON: store.fromJSON.bind(store),
		getUserLid: store.getUserLid.bind(store),
		loadMessage: store.loadMessage.bind(store),
		customQuery: store.customQuery.bind(store),
		getAllChats: store.getAllChats.bind(store),
		getChatById: store.getChatById.bind(store),
		getGroupByJid: store.getGroupByJid.bind(store),
		removeAllData: store.removeAllData.bind(store),
		storeUserData: store.storeUserData.bind(store),
		getAllContacts: store.getAllContacts.bind(store),
		getContactById: store.getContactById.bind(store),
		clearGroupsData: store.clearGroupsData.bind(store),
		storeStatusUpdate: store.storeStatusUpdate.bind(store),
		cleanupStatusData: store.cleanupStatusData.bind(store),
		fetchGroupMetadata: store.fetchGroupMetadata.bind(store),
		getAllSavedContacts: store.getAllSavedContacts.bind(store),
		loadAllGroupsMetadata: store.loadAllGroupsMetadata.bind(store),
		getRecentStatusUpdates: store.getRecentStatusUpdates.bind(store),
		fetchAllGroupsMetadata: store.fetchAllGroupsMetadata.bind(store)
	}
}
