import {
  Chat,
  Contact,
  GroupMetadata,
  ConnectionState,
  GroupMetadataRow,
  GroupMetadataResult,
  BaileysEventEmitter,
} from "../Types";
import pino from "pino";
import { proto } from "../../WAProto";
import { Pool } from "mysql2/promise";
import type makeMDSocket from "../Socket";
import { OptimizedMySQLStore } from "./optimized-mysql-store";

type WASocket = ReturnType<typeof makeMDSocket>;

interface makeMySQLStoreFunc {
  state: ConnectionState | null;
  bind: (ev: BaileysEventEmitter) => Promise<void>;
  loadMessage: (id: string) => Promise<proto.IWebMessageInfo | undefined>;
  loadAllGroupsMetadata: () => Promise<GroupMetadata[]>;
  customQuery: (query: string, params?: any[]) => Promise<any>;
  getAllChats: () => Promise<Chat[]>;
  getAllContacts: () => Promise<Contact[]>;
  getAllSavedContacts: () => Promise<Contact[]>;
  fetchAllGroupsMetadata: (
    sock: WASocket | undefined
  ) => Promise<GroupMetadataResult>;
  getChatById: (jid: string) => Promise<Chat | undefined>;
  getContactById: (jid: string) => Promise<Contact | undefined>;
  getGroupByJid: (jid: string) => Promise<GroupMetadataRow | undefined>;
  removeAllData: () => Promise<void>;
  getRecentStatusUpdates: () => Promise<proto.IWebMessageInfo[]>;
  fetchGroupMetadata: (
    jid: string,
    sock: WASocket | undefined
  ) => Promise<GroupMetadata | null>;
  clearGroupsData: () => Promise<void>;
  toJSON: () => Promise<any>;
  fromJSON: (
    json: any
  ) => Promise<{ totalChatsAffected: number; totalContactsAffected: number }>;
  storeUserData: (jid: string, username: string | null) => Promise<void>;
}

export function makeMySQLStore(
  instance_id: string,
  pool: Pool,
  skippedGroups: string[],
  logger?: pino.Logger
): makeMySQLStoreFunc {
  if (!pool) throw new Error("No MySQL connection pool provided");

  const log = logger || pino({ level: "info" });
  const store = new OptimizedMySQLStore(pool, log, instance_id, skippedGroups);

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
        INDEX idx_instance_jid (instance_id, jid),
        UNIQUE(instance_id, jid, username)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,
    ];

    for (const query of schema) {
      try {
        await pool.query(query);
        log.info(`Schema operation executed: ${query.slice(0, 50)}...`);
      } catch (error) {
        log.error({ error, query }, "Failed to execute schema operation");
      }
    }
  };

  createTables().catch((err) =>
    log.error({ err }, "Failed to create database tables")
  );

  return {
    state: store.state,
    bind: store.bind.bind(store),
    toJSON: store.toJSON.bind(store),
    fromJSON: store.fromJSON.bind(store),
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
    fetchGroupMetadata: store.fetchGroupMetadata.bind(store),
    getAllSavedContacts: store.getAllSavedContacts.bind(store),
    loadAllGroupsMetadata: store.loadAllGroupsMetadata.bind(store),
    getRecentStatusUpdates: store.getRecentStatusUpdates.bind(store),
    fetchAllGroupsMetadata: store.fetchAllGroupsMetadata.bind(store),
  };
}
