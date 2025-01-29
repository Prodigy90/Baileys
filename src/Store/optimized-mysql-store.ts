import {
  isJidUser,
  jidNormalizedUser,
  isJidStatusBroadcast,
} from "../WABinary";
import {
  Chat,
  Contact,
  WAMessage,
  GroupMetadata,
  ConnectionState,
  GroupParticipant,
  GroupMetadataRow,
  GroupMetadataEntry,
  GroupMetadataResult,
  BaileysEventEmitter,
  messageTypeMap,
} from "../Types";
import pino from "pino";
import { toNumber } from "../Utils";
import { LRUCache } from "lru-cache";
import { utc } from "moment-timezone";
import { proto } from "../../WAProto";
import type makeMDSocket from "../Socket";
import { Pool, RowDataPacket } from "mysql2/promise";
import { BatchProcessor, DbHelpers } from "../Utils/batch-processor";
import { CacheWarmer } from "../Utils/cache-warmer";

type WASocket = ReturnType<typeof makeMDSocket>;

const CACHE_CONFIG = {
  MAX_SIZE: 5000,
  TTL: {
    // Fast-changing data
    MESSAGES: 1000 * 60 * 5,
    STATUS_UPDATES: 1000 * 60 * 5,

    // Medium-changing data
    CHATS: 1000 * 60 * 15,
    GROUP_METADATA: 1000 * 60 * 15,

    // Slow-changing data
    CONTACTS: 1000 * 60 * 30,
    USER_DATA: 1000 * 60 * 30,

    // Default fallback
    DEFAULT: 1000 * 60 * 15,
  },
};

export type CacheType = keyof typeof CACHE_CONFIG.TTL;

export function getTTL(cacheKey: string): number {
  if (cacheKey.startsWith("chat_")) return CACHE_CONFIG.TTL.CHATS;
  if (cacheKey.includes("user_")) return CACHE_CONFIG.TTL.USER_DATA;
  if (cacheKey.startsWith("msg_")) return CACHE_CONFIG.TTL.MESSAGES;
  if (cacheKey.startsWith("contact_")) return CACHE_CONFIG.TTL.CONTACTS;
  if (cacheKey.startsWith("group_")) return CACHE_CONFIG.TTL.GROUP_METADATA;
  if (cacheKey.startsWith("status_")) return CACHE_CONFIG.TTL.STATUS_UPDATES;

  return CACHE_CONFIG.TTL.DEFAULT;
}

export class OptimizedMySQLStore {
  private dbHelpers: DbHelpers;
  private cacheWarmer: CacheWarmer;
  private cache: LRUCache<string, any>;
  state: ConnectionState | null = null;
  private batchProcessor: BatchProcessor;

  constructor(
    private pool: Pool,
    private logger: pino.Logger,
    private instance_id: string,
    private skippedGroups: string[]
  ) {
    this.cache = new LRUCache({
      max: CACHE_CONFIG.MAX_SIZE,
      ttl: CACHE_CONFIG.TTL.DEFAULT,
      ttlAutopurge: true,
      updateAgeOnGet: true,
      ttlResolution: 1000,
      fetchMethod: async (key: string) => {
        const ttl = getTTL(key);
        this.cache.ttl = ttl;
        return null;
      },
    });
    this.logger = logger || pino({ level: "info" });
    this.batchProcessor = new BatchProcessor(pool, this.logger);
    this.dbHelpers = new DbHelpers(pool, this.logger, this.cache);
    this.cacheWarmer = new CacheWarmer(
      pool,
      this.cache,
      instance_id,
      this.logger
    );

    this.cacheWarmer
      .start()
      .catch((err) =>
        this.logger.error({ err }, "Failed to start cache warming")
      );
  }

  /**
   * Optimized getAllChats with pagination and caching
   */
  async getAllChats(): Promise<Chat[]> {
    try {
      const fetchBatch = async (offset: number, limit: number) => {
        const [rows] = await this.pool.query(
          `
        SELECT chat 
        FROM chats 
        WHERE instance_id = ? 
        ORDER BY conversation_timestamp DESC
        LIMIT ? OFFSET ?
      `,
          [this.instance_id, limit, offset]
        );

        return (rows as any[]).map((row) =>
          typeof row.chat === "string" ? JSON.parse(row.chat) : row.chat
        );
      };

      return await this.fetchAllWithPagination(fetchBatch, "chats");
    } catch (error) {
      this.logger.error({ error }, "getAllChats failed");
      return [];
    }
  }

  /**
   * Optimized getAllContacts with pagination and caching
   */
  async getAllContacts(): Promise<Contact[]> {
    try {
      const fetchBatch = async (offset: number, limit: number) => {
        const [rows] = await this.pool.query(
          `
        SELECT contact 
        FROM contacts 
        WHERE instance_id = ? 
        ORDER BY JSON_EXTRACT(contact, '$.name') ASC
        LIMIT ? OFFSET ?
        `,
          [this.instance_id, limit, offset]
        );

        return (rows as any[]).map((row) =>
          typeof row.contact === "string"
            ? JSON.parse(row.contact)
            : row.contact
        );
      };

      return await this.fetchAllWithPagination(fetchBatch, "contacts");
    } catch (error) {
      this.logger.error({ error }, "getAllContacts failed");
      return [];
    }
  }

  async fetchAllWithPagination<T>(
    fetchFunction: (offset: number, limit: number) => Promise<T[]>,
    table: string,
    batchSize: number = 500
  ): Promise<T[]> {
    const cacheKey = `${table}_${this.instance_id}_all`;

    const cachedResult = await this.dbHelpers.getFromCacheOrDb(
      cacheKey,
      "",
      [],
      (row) => row as T[]
    );

    if (cachedResult) {
      this.logger.info(`Returning cached results for ${table}`);
      return cachedResult;
    }

    const [countResult] = await this.pool.query(
      `SELECT COUNT(*) as total FROM ${table} WHERE instance_id = ?`,
      [this.instance_id]
    );
    const totalItems = (countResult as any)[0].total;

    if (totalItems === 0) {
      return [];
    }

    const allItems: T[] = [];
    let processedItems = 0;

    while (processedItems < totalItems) {
      const batch = await fetchFunction(processedItems, batchSize);
      allItems.push(...batch);
      processedItems += batchSize;
    }
    this.cache.set(cacheKey, allItems);
    this.logger.info(`Cached ${totalItems} ${table} results`);
    return allItems;
  }

  async toJSON() {
    const [chats, contacts, messages, labels, labelAssociations] =
      (await Promise.all([
        this.dbHelpers.getFromCacheOrDb(
          `${this.instance_id}_all_chats`,
          "SELECT * FROM chats WHERE instance_id = ?",
          [this.instance_id],
          (rows) =>
            Array.isArray(rows)
              ? rows.map((row) =>
                  typeof row.chat === "string" ? JSON.parse(row.chat) : row.chat
                )
              : []
        ),
        this.dbHelpers.getFromCacheOrDb(
          `${this.instance_id}_all_contacts`,
          "SELECT * FROM contacts WHERE instance_id = ?",
          [this.instance_id],
          (rows) =>
            Array.isArray(rows)
              ? rows.map((row) =>
                  typeof row.contact === "string"
                    ? JSON.parse(row.contact)
                    : row.contact
                )
              : []
        ),
        this.dbHelpers.getFromCacheOrDb(
          `${this.instance_id}_all_messages`,
          "SELECT * FROM messages WHERE instance_id = ?",
          [this.instance_id],
          (rows) =>
            Array.isArray(rows)
              ? rows.map((row) =>
                  typeof row.message_data === "string"
                    ? JSON.parse(row.message_data)
                    : row.message_data
                )
              : []
        ),
        this.dbHelpers.getFromCacheOrDb(
          `${this.instance_id}_all_labels`,
          "SELECT * FROM labels WHERE instance_id = ?",
          [this.instance_id],
          (rows) =>
            Array.isArray(rows)
              ? rows.map((row) =>
                  typeof row.label === "string"
                    ? JSON.parse(row.label)
                    : row.label
                )
              : []
        ),
        this.dbHelpers.getFromCacheOrDb(
          `${this.instance_id}_all_label_associations`,
          "SELECT * FROM label_associations WHERE instance_id = ?",
          [this.instance_id],
          (rows) =>
            Array.isArray(rows)
              ? rows.map((row) =>
                  typeof row.association === "string"
                    ? JSON.parse(row.association)
                    : row.association
                )
              : []
        ),
      ])) || [];

    return {
      chats: chats || [],
      labels: labels || [],
      contacts: contacts || [],
      messages: messages || [],
      labelAssociations: labelAssociations || [],
    };
  }

  /**
   * Optimized bulk data import with batch processing
   */
  async fromJSON(json: {
    chats: Chat[];
    contacts: Contact[];
    messages: { [id: string]: WAMessage[] };
  }): Promise<{ totalChatsAffected: number; totalContactsAffected: number }> {
    const { chats, contacts } = json;
    let totalChatsAffected = 0;
    let totalContactsAffected = 0;

    try {
      const filteredChats = chats
        .filter((chat) => isJidUser(chat.id))
        .map((chat) => ({
          instance_id: this.instance_id,
          jid: chat.id,
          chat: { ...chat, messages: [] },
        }));

      const filteredContacts = contacts
        .filter((contact) => isJidUser(contact.id))
        .map((contact) => ({
          instance_id: this.instance_id,
          jid: contact.id,
          contact: contact,
        }));

      filteredChats.forEach((chat) => {
        this.batchProcessor.queueItem("chats", chat);
        totalChatsAffected++;
      });

      filteredContacts.forEach((contact) => {
        this.batchProcessor.queueItem("contacts", contact);
        totalContactsAffected++;
      });

      return { totalChatsAffected, totalContactsAffected };
    } catch (error) {
      this.logger.error({ error }, "fromJSON failed");
      throw error;
    }
  }

  /**
   * Optimized removeAllData with parallel processing
   */
  async removeAllData(): Promise<void> {
    try {
      const tables = [
        "chats",
        "contacts",
        "messages",
        "users",
        "groups_metadata",
        "groups_status",
      ];

      await Promise.all(
        tables.map((table) =>
          this.pool.query(`DELETE FROM ${table} WHERE instance_id = ?`, [
            this.instance_id,
          ])
        )
      );

      this.cache.clear();
      this.logger.info(
        { instance_id: this.instance_id },
        "All data removed successfully"
      );
    } catch (error) {
      this.logger.error({ error }, "removeAllData failed");
      throw error;
    }
  }

  async loadMessage(id: string): Promise<proto.IWebMessageInfo | undefined> {
    if (!id) {
      throw new Error("Invalid id");
    }

    const messageSql =
      "SELECT message_data from messages WHERE instance_id = ? and message_id = ?";
    const message_rows = await this.dbHelpers.getFromCacheOrDb(
      `msg_${this.instance_id}_${id}`,
      messageSql,
      [this.instance_id, id],
      (row) => row.message_data
    );

    return message_rows || undefined;
  }

  getMessageType = (message: proto.IWebMessageInfo): string => {
    const messageType = Object.keys(message.message || {})[0];
    return messageTypeMap[messageType] || "unknown";
  };

  async getStatusInDBResult(id: string): Promise<Boolean> {
    try {
      const statusInDBSql = `
      SELECT EXISTS(
        SELECT 1 
        FROM status_updates 
        WHERE status_id = ? AND instance_id = ?
      ) AS exists_flag;
    `;

      const statusInDBResult = await this.customQuery(statusInDBSql, [
        id,
        this.instance_id,
      ]);
      return statusInDBResult[0].exists_flag === 1;
    } catch (error) {
      this.logger.error({ error }, "Error checking if status is in db");
      return false;
    }
  }

  async getUserData(): Promise<any | null> {
    return await this.dbHelpers.getFromCacheOrDb(
      `${this.instance_id}_user_cache`,
      `SELECT * FROM users WHERE instance_id = ?`,
      [this.instance_id],
      (row) => ({ username: row.username, jid: row.jid })
    );
  }

  async isUserGroupAdmin(id: string): Promise<Boolean> {
    return (
      (await this.dbHelpers.getFromCacheOrDb(
        `admin_${this.instance_id}_${id}`,
        "SELECT is_admin FROM groups_metadata WHERE instance_id = ? AND jid = ?",
        [this.instance_id, id],
        (row) => row.is_admin === 1
      )) || false
    );
  }

  async isUserAdminOrSuperAdmin(participants: GroupParticipant[]) {
    const userData = await this.getUserData();
    if (!userData) return false;

    return participants.some(
      (participant) =>
        participant.id === userData.jid &&
        (participant.admin === "superadmin" || participant.admin === "admin")
    );
  }

  async hasGroups(): Promise<boolean> {
    return (
      (await this.dbHelpers.getFromCacheOrDb(
        `${this.instance_id}_hasGroups`,
        "SELECT status FROM groups_status WHERE instance_id = ?",
        [this.instance_id],
        (row) => row.status === 1
      )) || false
    );
  }

  async getChatById(jid: string): Promise<Chat | undefined> {
    return await this.dbHelpers.getFromCacheOrDb(
      `chat_${this.instance_id}_${jid}`,
      `SELECT chat FROM chats WHERE instance_id = ? AND jid = ?`,
      [this.instance_id, jid],
      (row) => row.chat
    );
  }

  async getContactById(jid: string): Promise<Contact | undefined> {
    return await this.dbHelpers.getFromCacheOrDb(
      `contact_${this.instance_id}_${jid}`,
      `SELECT contact FROM contacts WHERE instance_id = ? AND jid = ?`,
      [this.instance_id, jid],
      (row) => row.contact
    );
  }

  async getGroupByJid(jid: string): Promise<GroupMetadataRow | null> {
    return await this.dbHelpers.getFromCacheOrDb(
      `group_${this.instance_id}_${jid}`,
      `SELECT * FROM groups_metadata WHERE instance_id = ? AND jid = ?`,
      [this.instance_id, jid],
      (row): GroupMetadataRow | null => {
        if (!row) return null;

        return {
          subject: row.subject,
          metadata: row.metadata,
          is_admin: row.is_admin,
          group_index: row.group_index,
          admin_index: row.admin_index,
          participating: row.participating,
        };
      }
    );
  }

  async customQuery(query: string, params?: any[]): Promise<any> {
    try {
      const [result] = await this.pool.query(query, params);
      return result;
    } catch (error) {
      this.logger.error({ error, query }, "Failed to execute custom query");
      throw error;
    }
  }

  async storeUserData(jid: string, username: string | null): Promise<void> {
    try {
      const userSQL = `
      INSERT INTO users (instance_id, jid, username)
      VALUES (?, ?, ?)
      ON DUPLICATE KEY UPDATE username = VALUES(username)`;
      await this.customQuery(userSQL, [this.instance_id, jid, username]);
      this.logger.info(
        { jid, username },
        "User data stored/updated successfully"
      );

      this.cache.delete(`${this.instance_id}_user_cache`);
    } catch (error) {
      this.logger.error({ error, jid }, "Failed to insert/update user data");
    }
  }

  async getAllSavedContacts(): Promise<Contact[]> {
    try {
      const sql = `
        SELECT
          jid AS id,
          JSON_UNQUOTE(JSON_EXTRACT(contact, '$.name')) AS name
        FROM
          contacts
        WHERE
          instance_id = ?
          AND jid LIKE '%@s.whatsapp.net'
          AND
          JSON_UNQUOTE(JSON_EXTRACT(contact, '$.name')) IS NOT NULL;
      `;

      return await this.customQuery(sql, [this.instance_id]);
    } catch (error) {
      this.logger.error(
        { error, key: { instanceId: this.instance_id } },
        "Failed to retrieve saved contacts"
      );
      throw error;
    }
  }

  async getRecentStatusUpdates(): Promise<proto.IWebMessageInfo[]> {
    const messages: proto.IWebMessageInfo[] = [];
    try {
      const [rows] = await this.pool.query<RowDataPacket[]>(
        `SELECT status_message, view_count, message_type, post_date 
       FROM status_updates
       WHERE instance_id = ?
         AND post_date >= NOW() - INTERVAL 24 HOUR
       ORDER BY post_date ASC`,
        [this.instance_id]
      );

      for (const row of rows) {
        try {
          const message =
            typeof row.status_message === "object"
              ? row.status_message
              : JSON.parse(row.status_message.toString());

          const postDate = new Date(row.post_date);
          const formattedTime = `${postDate
            .getHours()
            .toString()
            .padStart(2, "0")}:${postDate
            .getMinutes()
            .toString()
            .padStart(2, "0")}`;

          message.post_time = formattedTime;
          message.view_count = row.view_count;
          message.message_type = row.message_type;

          messages.push(message);
        } catch (parseError) {
          this.logger.error(
            { error: parseError },
            "Failed to parse status message data"
          );
        }
      }
    } catch (error) {
      this.logger.error(
        { error, instance_id: this.instance_id },
        "Error fetching recent status updates"
      );
    }
    return messages;
  }

  async loadAllGroupsMetadata(): Promise<GroupMetadata[]> {
    const inDB = await this.hasGroups();
    if (!inDB) return [];

    const cacheKey = `${this.instance_id}_all_groups_metadata`;
    if (this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey) as GroupMetadata[];
    }

    try {
      const rows = await this.customQuery(
        "SELECT metadata FROM groups_metadata WHERE instance_id = ?",
        [this.instance_id]
      );

      const metadata = rows
        .map((row: any) => {
          try {
            return typeof row.metadata === "object"
              ? row.metadata
              : JSON.parse(row.metadata);
          } catch (error) {
            this.logger.error(
              { error, metadata: row.metadata },
              "Failed to parse group metadata"
            );
            return null;
          }
        })
        .filter(Boolean) as GroupMetadata[];

      this.cache.set(cacheKey, metadata);
      return metadata;
    } catch (error) {
      this.logger.error(
        { error, instance_id: this.instance_id },
        "Failed to load group metadata"
      );
      return [];
    }
  }
  async clearGroupsData(): Promise<void> {
    try {
      await Promise.all([
        this.customQuery(`DELETE FROM groups_metadata WHERE instance_id = ?`, [
          this.instance_id,
        ]),
        this.customQuery(`DELETE FROM groups_status WHERE instance_id = ?`, [
          this.instance_id,
        ]),
      ]);

      const cacheKeys = [
        `${this.instance_id}_all_groups_metadata`,
        `${this.instance_id}_hasGroups`,
      ];
      cacheKeys.forEach((key) => this.cache.delete(key));

      this.logger.info(
        { instance_id: this.instance_id },
        "Groups data cleared successfully"
      );
    } catch (error) {
      this.logger.error(
        { error, instance_id: this.instance_id },
        "Failed to clear groups data"
      );
      throw error;
    }
  }

  async fetchGroupMetadata(
    jid: string,
    sock: WASocket | undefined
  ): Promise<GroupMetadata | null> {
    if (!sock) throw new Error("WASocket is undefined");

    return (
      (await this.dbHelpers.getFromCacheOrDb(
        `group_metadata_${this.instance_id}_${jid}`,
        `SELECT metadata FROM groups_metadata WHERE instance_id = ? AND jid = ?`,
        [this.instance_id, jid],
        async (row) => {
          if (row) {
            return row.metadata;
          }

          this.logger.info(
            { key: { jid } },
            "Group not found in database, fetching from WASocket"
          );
          const metadata = await sock.groupMetadata(jid);

          if (!metadata) {
            this.logger.error(
              { key: { jid } },
              "Failed to fetch group metadata from WASocket"
            );
            return null;
          }

          await this.customQuery(
            `INSERT INTO groups_metadata (instance_id, jid, metadata) VALUES (?, ?, ?)`,
            [this.instance_id, jid, metadata]
          );

          return metadata;
        }
      )) || null
    );
  }

  async fetchAllGroupsMetadata(
    sock: WASocket | undefined
  ): Promise<GroupMetadataResult> {
    const inDB = await this.hasGroups();

    if (inDB) {
      try {
        const [allGroups, adminGroups] = await Promise.all([
          this.customQuery(
            `SELECT jid as id, subject, group_index AS groupIndex
           FROM groups_metadata
           WHERE instance_id = ?
           AND participating = 1
           ORDER BY group_index ASC`,
            [this.instance_id]
          ),
          this.customQuery(
            `SELECT jid as id, subject, admin_index AS adminIndex, 
                 JSON_EXTRACT(metadata, '$.participants') AS participants
           FROM groups_metadata
           WHERE instance_id = ? AND is_admin = 1 AND participating = 1
           ORDER BY admin_index ASC`,
            [this.instance_id]
          ),
        ]);

        return {
          allGroups: allGroups.map((group: any) => ({
            id: group.id,
            subject: group.subject,
            groupIndex: group.groupIndex,
          })),
          adminGroups: adminGroups.map((group: any) => ({
            id: group.id,
            subject: group.subject,
            adminIndex: group.adminIndex,
            participants: group.participants.map((p: any) => p.id) || [],
          })),
        };
      } catch (error) {
        this.logger.error(
          { error },
          "Failed to fetch groups metadata from database"
        );
        throw error;
      }
    } else {
      if (!sock) throw new Error("WASocket is undefined");

      try {
        const groups = await sock.groupFetchAllParticipating();
        const sortedGroups = Object.entries(groups).sort(([_, a], [__, b]) =>
          (a.subject || a.id).localeCompare(b.subject || b.id)
        );

        let groupIndex = 0;
        let adminIndex = 0;
        const allGroups: { id: string; subject: string; groupIndex: number }[] =
          [];
        const adminGroups: {
          id: string;
          subject: string;
          participants: string[];
          adminIndex: number;
        }[] = [];

        const groupMetadata: GroupMetadataEntry[] = [];

        for (const [id, metadata] of sortedGroups) {
          const {
            subject,
            announce,
            isCommunity,
            participants,
            isCommunityAnnounce,
          } = metadata;
          const admin = await this.isUserAdminOrSuperAdmin(participants);

          if (
            (this.skippedGroups.includes(id) && !admin) ||
            (isCommunity && !announce && !isCommunityAnnounce) ||
            (announce && !isCommunity && isCommunityAnnounce && !admin)
          ) {
            continue;
          }

          groupIndex++;
          const name =
            subject.length > 0 ? subject : `Unnamed Group ${groupIndex}`;
          allGroups.push({ id, subject: name, groupIndex });

          if (admin) {
            adminIndex++;
            adminGroups.push({
              id,
              subject: name,
              participants: participants.map((p) => p.id),
              adminIndex,
            });
          }

          groupMetadata.push({
            id,
            metadata,
            groupIndex,
            subject: name,
            isAdmin: admin,
            adminIndex: admin ? adminIndex : 0,
          });
        }

        if (groupMetadata.length > 0) {
          await this.customQuery(
            `INSERT INTO groups_metadata 
           (instance_id, jid, subject, is_admin, group_index, admin_index, metadata)
           VALUES ?`,
            [
              groupMetadata.map((g) => [
                this.instance_id,
                g.id,
                g.subject,
                g.isAdmin,
                g.groupIndex,
                g.adminIndex,
                JSON.stringify(g.metadata),
              ]),
            ]
          );

          await this.customQuery(
            `INSERT INTO groups_status (instance_id, status, group_index, admin_index)
           VALUES (?, TRUE, ?, ?)
           ON DUPLICATE KEY UPDATE
           group_index = group_index + VALUES(group_index),
           admin_index = admin_index + VALUES(admin_index)`,
            [this.instance_id, groupIndex, adminIndex]
          );
        }

        return { allGroups, adminGroups };
      } catch (error) {
        this.logger.error(
          { error },
          "Failed to fetch and process group metadata"
        );
        throw error;
      }
    }
  }

  async bind(ev: BaileysEventEmitter): Promise<void> {
    ev.on("connection.update", async (update) => {
      Object.assign(this.state || {}, update);
    });

    ev.on(
      "messaging-history.set",
      async ({ chats, contacts }: { chats: Chat[]; contacts: Contact[] }) => {
        try {
          await Promise.all([
            (async () => {
              const filteredChats = chats
                .filter(
                  (chat) =>
                    isJidUser(chat.id) &&
                    !chat.messages?.some(
                      (m) => !m.message?.message && m.message?.messageStubType
                    )
                )
                .map((chat) => ({
                  instance_id: this.instance_id,
                  jid: chat.id,
                  chat: { ...chat, messages: [] },
                }));

              filteredChats.forEach((chat) =>
                this.batchProcessor.queueItem("chats", chat)
              );
            })(),

            (async () => {
              const filteredContacts = contacts
                .filter((contact) => isJidUser(contact.id))
                .map((contact) => ({
                  instance_id: this.instance_id,
                  jid: contact.id,
                  contact: contact,
                }));

              filteredContacts.forEach((contact) =>
                this.batchProcessor.queueItem("contacts", contact)
              );
            })(),
          ]);
        } catch (error) {
          this.logger.error(
            { error, instance_id: this.instance_id },
            "Failed to process messaging history"
          );
        }
      }
    );

    ev.on("chats.upsert", async (chats: Chat[]) => {
      try {
        const filteredChats = chats
          .filter(
            (chat) =>
              isJidUser(chat.id) &&
              !chat.messages?.some(
                (m) => !m.message?.message && m.message?.messageStubType
              )
          )
          .map((chat) => ({
            instance_id: this.instance_id,
            jid: chat.id,
            chat: { ...chat, messages: [] },
          }));

        filteredChats.forEach((chat) =>
          this.batchProcessor.queueItem("chats", chat)
        );
      } catch (error) {
        this.logger.error({ error }, "Failed to handle chats upsert");
      }
    });

    ev.on("contacts.upsert", async (contacts: Contact[]) => {
      try {
        contacts
          .filter((contact) => isJidUser(contact.id))
          .forEach((contact) => {
            this.batchProcessor.queueItem("contacts", {
              instance_id: this.instance_id,
              jid: contact.id,
              contact: contact,
            });
          });
      } catch (error) {
        this.logger.error({ error }, "Failed to handle contacts upsert");
      }
    });

    ev.on(
      "messages.upsert",
      async ({ messages, type }: { messages: WAMessage[]; type: string }) => {
        try {
          await Promise.all(
            messages.map(async (message) => {
              if (!message.key?.id) return;

              const localTime = utc(new Date())
                .tz("Africa/Lagos")
                .format("YYYY-MM-DD HH:mm:ss");

              if (message.key.fromMe) {
                this.batchProcessor.queueItem(
                  "messages",
                  {
                    instance_id: this.instance_id,
                    message_id: message.key.id,
                    message_data: message,
                    post_date: localTime,
                  },
                  2
                );

                if (
                  isJidStatusBroadcast(message.key.remoteJid as string) &&
                  !message.message?.reactionMessage
                ) {
                  const messageType = this.getMessageType(message);
                  if (messageType !== "unknown") {
                    this.batchProcessor.queueItem(
                      "status_updates",
                      {
                        instance_id: this.instance_id,
                        status_id: message.key.id,
                        status_message: message,
                        post_date: localTime,
                        message_type: messageType,
                      },
                      3
                    );
                  }
                }
              } else if (isJidUser(message.key.remoteJid as string)) {
                const remoteJid = message.key.remoteJid as string;
                const [chat, contact] = await Promise.all([
                  this.getChatById(remoteJid),
                  this.getContactById(remoteJid),
                ]);

                if (
                  contact &&
                  message.pushName &&
                  (!contact?.notify || contact?.notify === "")
                ) {
                  this.batchProcessor.queueItem("contacts", {
                    instance_id: this.instance_id,
                    jid: remoteJid,
                    contact: {
                      ...contact,
                      notify: message.pushName,
                    },
                  });
                }

                if (!chat) {
                  this.batchProcessor.queueItem("chats", {
                    instance_id: this.instance_id,
                    jid: remoteJid,
                    chat: {
                      id: remoteJid,
                      conversationTimestamp: toNumber(message.messageTimestamp),
                      unreadCount: 1,
                    },
                  });
                }

                if (!contact) {
                  this.batchProcessor.queueItem("contacts", {
                    instance_id: this.instance_id,
                    jid: remoteJid,
                    contact: {
                      id: remoteJid,
                      notify: message.pushName || "",
                    },
                  });
                }
              }
            })
          );
        } catch (error) {
          this.logger.error(
            { error, key: { instanceId: this.instance_id } },
            "Failed to handle message upserts"
          );
        }
      }
    );

    ev.on(
      "message-receipt.update",
      async (updates: { key: proto.IMessageKey }[]) => {
        try {
          await Promise.all(
            updates.map(async (update) => {
              if (
                update.key.id &&
                update.key.fromMe &&
                isJidStatusBroadcast(update.key.remoteJid as string)
              ) {
                const inDB = await this.getStatusInDBResult(update.key.id);
                if (!inDB) return;
                const viewer_jid = jidNormalizedUser(
                  update.key.participant as string
                );

                const viewerCacheKey = `${update.key.id}_${viewer_jid}`;
                if (this.cache.has(viewerCacheKey)) return;

                this.batchProcessor.queueItem("status_viewers", {
                  instance_id: this.instance_id,
                  status_id: update.key.id,
                  viewer_jid,
                  view_date: new Date()
                    .toISOString()
                    .slice(0, 19)
                    .replace("T", " "),
                });

                const exists = await this.dbHelpers.checkExists(
                  viewerCacheKey,
                  "SELECT 1 FROM status_viewers WHERE status_id = ? AND viewer_jid = ? LIMIT 1",
                  [update.key.id, update.key.participant]
                );

                if (!exists) {
                  this.cache.set(viewerCacheKey, true);
                  this.batchProcessor.queueItem("status_updates", {
                    id: update.key.id,
                    view_count_increment: 1,
                  });
                }
              }
            })
          );
        } catch (error) {
          this.logger.error(
            { error, key: { instanceId: this.instance_id } },
            "Failed to handle message-receipt updates"
          );
        }
      }
    );

    ev.on("groups.update", async (updates: Partial<GroupMetadata>[]) => {
      try {
        const active = await this.hasGroups();
        if (!active) return;

        await Promise.all(
          updates.map(async (update) => {
            if (!update.id || !update.subject) return;

            const groupData = await this.getGroupByJid(update.id);
            if (!groupData) {
              this.logger.warn(
                { groupId: update.id },
                "No metadata found for group"
              );
              return;
            }

            const metadata = groupData.metadata;
            metadata.subject = update.subject;

            this.batchProcessor.queueItem("groups_metadata", {
              instance_id: this.instance_id,
              jid: update.id,
              subject: update.subject,
              is_admin: groupData.is_admin,
              participating: groupData.participating,
              group_index: groupData.group_index,
              admin_index: groupData.admin_index,
              metadata: metadata,
            });
          })
        );
      } catch (error) {
        this.logger.error({ error }, "Failed to handle group updates");
      }
    });

    ev.on("groups.upsert", async (groupMetadata: GroupMetadata[]) => {
      try {
        const active = await this.hasGroups();
        if (!active) return;

        const conn = await this.pool.getConnection();
        try {
          await conn.beginTransaction();

          const [statusRows] = await conn.query<RowDataPacket[]>(
            "SELECT group_index, admin_index FROM groups_status WHERE instance_id = ?",
            [this.instance_id]
          );

          if (!statusRows.length) return;

          let { group_index, admin_index } = statusRows[0];

          const newGroups = await Promise.all(
            groupMetadata.map(async (group) => {
              const {
                id,
                announce,
                isCommunity,
                participants,
                isCommunityAnnounce,
              } = group;
              const admin = await this.isUserAdminOrSuperAdmin(participants);

              if (
                (this.skippedGroups.includes(id) && !admin) ||
                (isCommunity && !announce && !isCommunityAnnounce) ||
                (announce && !isCommunity && isCommunityAnnounce && !admin)
              ) {
                return null;
              }
              return { group, admin };
            })
          );

          const validGroups = newGroups.filter((g) => g !== null);
          const newAdminCount = validGroups.filter((g) => g?.admin).length;

          await conn.query(
            `UPDATE groups_status 
             SET group_index = group_index + ?, 
                 admin_index = admin_index + ?
             WHERE instance_id = ?`,
            [validGroups.length, newAdminCount, this.instance_id]
          );

          await conn.commit();

          validGroups.forEach((groupData, idx) => {
            if (!groupData) return;
            const { group, admin } = groupData;
            const currentGroupIndex = group_index + idx + 1;
            const currentAdminIndex = admin ? admin_index + idx + 1 : null;

            this.batchProcessor.queueItem("groups_metadata", {
              instance_id: this.instance_id,
              jid: group.id,
              subject: group.subject || `unnamed Group ${currentGroupIndex}`,
              is_admin: admin,
              participating: true,
              group_index: currentGroupIndex,
              admin_index: currentAdminIndex,
              metadata: group,
            });
          });
        } catch (error) {
          await conn.rollback();
          throw error;
        } finally {
          conn.release();
        }
      } catch (error) {
        this.logger.error({ error }, "Failed to handle group upserts");
      }
    });

    ev.on("group-participants.update", async ({ id, participants, action }) => {
      try {
        const active = await this.hasGroups();
        if (!active) return;

        const [{ jid }, is_group_admin] = await Promise.all([
          this.getUserData(),
          this.isUserGroupAdmin(id),
        ]);

        if (
          !is_group_admin &&
          !(participants.includes(jid) && action === "promote")
        ) {
          return;
        }

        const groupData = await this.getGroupByJid(id);
        if (!groupData) {
          this.logger.warn({ groupId: id }, "No metadata found for group");
          return;
        }

        let is_admin = is_group_admin;
        const metadata = groupData.metadata;
        let participating = groupData.participating;
        let currentAdminIndex = groupData.admin_index;

        switch (action) {
          case "add":
            metadata.participants.push(
              ...participants.map((id) => ({
                id,
                isAdmin: false,
                isSuperAdmin: false,
              }))
            );
            break;

          case "promote":
          case "demote":
            if (!is_group_admin && action === "promote") {
              const adminResult = await this.customQuery(
                "SELECT admin_index FROM groups_status WHERE instance_id = ?",
                [this.instance_id]
              );

              const admin_index = adminResult?.[0]?.admin_index ?? 0;

              if (admin_index > 0) {
                currentAdminIndex = admin_index + 1;

                const updateAdminIndexSql = `
                  UPDATE groups_status
                  SET admin_index = admin_index + 1
                  WHERE instance_id = ?
                `;
                await this.customQuery(updateAdminIndexSql, [this.instance_id]);
              }
            }

            metadata.participants = metadata.participants.map(
              (participant) => ({
                ...participant,
                isAdmin: participants.includes(participant.id)
                  ? action === "promote"
                  : participant.isAdmin,
              })
            );

            is_admin = action === "promote";
            break;

          case "remove":
            metadata.participants = metadata.participants.filter(
              (participant) => !participants.includes(participant.id)
            );

            if (participants.includes(jid)) {
              participating = false;
              is_admin = false;
            }
            break;
        }

        this.batchProcessor.queueItem("groups_metadata", {
          instance_id: this.instance_id,
          jid: id,
          subject: groupData.subject,
          is_admin,
          participating,
          group_index: groupData.group_index,
          admin_index: currentAdminIndex,
          metadata: metadata,
        });
      } catch (error) {
        this.logger.error(
          { error },
          "Failed to handle group participants update"
        );
      }
    });
  }
}
