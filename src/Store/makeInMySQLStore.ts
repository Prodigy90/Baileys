import type {
  Chat,
  Contact,
  WAMessage,
  PresenceData,
  WAMessageKey,
  GroupMetadata,
  ConnectionState,
  BaileysEventEmitter,
  GroupParticipant,
  ParticipantAction
} from "../Types";
import pino from "pino";
import { LRUCache } from "lru-cache";
import { proto } from "../../WAProto";
import { Label } from "../Types/Label";
import type makeMDSocket from "../Socket";
import { isJidGroup, isJidStatusBroadcast, isJidUser } from "../WABinary";
import { LabelAssociation } from "../Types/LabelAssociation";
import {
  Pool,
  RowDataPacket,
  ResultSetHeader,
  QueryResult
} from "mysql2/promise";
import { toNumber } from "../Utils";

type WASocket = ReturnType<typeof makeMDSocket>;

const messageTypeMap: Record<string, string> = {
  extendedTextMessage: "text",
  audioMessage: "audio",
  videoMessage: "video",
  imageMessage: "image"
};

interface GroupMetadataEntry {
  id: string;
  subject: string;
  isAdmin: boolean;
  groupIndex: number;
  adminIndex: number;
  metadata: GroupMetadata;
}

interface GroupMetadataResult {
  allGroups: {
    id: string;
    subject: string;
    groupIndex: number;
  }[];
  adminGroups: {
    id: string;
    subject: string;
    adminIndex: number;
    participants: string[];
  }[];
}

interface makeMySQLStoreFunc {
  state: ConnectionState | null;
  bind: (ev: BaileysEventEmitter) => Promise<void>;
  getData: (key: string) => Promise<any>;
  setData: (key: string, data: any) => Promise<void>;
  loadMessages: (
    jid: string,
    count: number
  ) => Promise<proto.IWebMessageInfo[]>;
  loadMessage: (id: string) => Promise<proto.IWebMessageInfo | undefined>;
  loadAllGroupMetadata: () => Promise<GroupMetadata[]>;
  loadGroupMetadataByJid: (jid: string) => Promise<GroupMetadata | undefined>;
  customQuery: (query: string, params?: any[]) => Promise<any>;
  getAllChats: () => Promise<Chat[]>;
  getAllContacts: () => Promise<Contact[]>;
  getAllSavedContacts: () => Promise<Contact[]>;
  fetchAllGroupsMetadata: (
    sock: WASocket | undefined
  ) => Promise<GroupMetadataResult>;
  getChatById: (jid: string) => Promise<Chat | undefined>;
  getContactById: (jid: string) => Promise<Contact | undefined>;
  getGroupByJid: (jid: string) => Promise<GroupMetadata | undefined>;
  updateMessageStatus: (
    jid: string,
    id: string,
    status: "sent" | "delivered" | "read"
  ) => Promise<void>;
  removeAllData: () => Promise<void>;
  getMessageLabels: (messageId: string) => Promise<string[]>;
  getRecentStatusUpdates: () => Promise<proto.IWebMessageInfo[]>;
  mostRecentMessage: (jid: string) => Promise<proto.IWebMessageInfo>;
  fetchImageUrl: (
    jid: string,
    sock: WASocket | undefined
  ) => Promise<string | null | undefined>;
  fetchGroupMetadata: (
    jid: string,
    sock: WASocket | undefined
  ) => Promise<GroupMetadata | null>;
  clearGroupsData: () => Promise<void>;
  fetchMessageReceipts: ({
    remoteJid,
    id
  }: WAMessageKey) => Promise<proto.IUserReceipt[] | null | undefined>;
  toJSON: () => any;
  fromJSON: (json: any) => void;
  storeUserData: (jid: string, username: string | null) => Promise<void>;
}

/**
 * @typedef {Object} makeMySQLStoreFunc
 * @description A MySQL-based store for managing Baileys  messages, groups, and contacts persistently.
 * Provides functionality similar to `makeInMemoryStore` but stores everything in a MySQL database.
 *
 * @property {ConnectionState | null} state - The current connection state, including authentication and session information.
 *
 * @property {function(BaileysEventEmitter): Promise<void>} bind -
 * Binds Baileys event listeners for handling socket events like message receipt, connection updates, and more.
 *
 * @property {function(string): Promise<any>} getData -
 * Retrieves a specific piece of data from the MySQL store using a unique key (such as session or connection data).
 *
 * @property {function(string, any): Promise<void>} setData -
 * Stores a key-value pair in the MySQL store (used for saving session data, settings, etc.).
 *
 * @property {function(string, number): Promise<proto.IWebMessageInfo[]>} loadMessages -
 * Loads a specified number of messages for a given chat (by chat ID) from the MySQL store.
 *
 * @property {function(string): Promise<proto.IWebMessageInfo | undefined>} loadMessage -
 * Loads a specific message by its message ID and instance_id from the MySQL store.
 *
 * @property {function(): Promise<GroupMetadata[]>} loadAllGroupMetadata -
 * Retrieves metadata for all groups from the MySQL store.
 *
 * @property {function(string): Promise<GroupMetadata | undefined>} loadGroupMetadataByJid -
 * Loads metadata for a specific group using its JID.
 *
 * @property {function(string, any[]): Promise<any>} customQuery -
 * Executes a custom SQL query with optional parameters, enabling advanced interaction with the MySQL database.
 *
 * @property {function(): Promise<Contact[]>} getAllContacts -
 * Retrieves a list of all contacts stored in the MySQL store.
 *
 * @property {function(): Promise<Contact[]>} getAllContacts -
 * Retrieves a list of all saved contacts stored in the MySQL store.
 *
 * @property {function(): Promise<Chat[]>} getAllChats -
 * Retrieves a list of all contacts stored in the MySQL store.
 *
 * @property {function(string): Promise<Chat | undefined>} getChatById -
 * Retrieves a chat from store by jid
 *
 * @property {function(string): Promise<Contact | undefined>} getContactById -
 * Retrieves a contact from store by jid
 *
 * @property {function(string): Promise<GroupMetadata | undefined>} getGroupByJid -
 * Retrieves metadata for a specific group using its JID.
 *
 * @property {function(string, string, string): Promise<void>} updateMessageStatus -
 * Updates the status of a message (sent, delivered, or read) in the MySQL store.
 *
 * @property {function(): Promise<void>} removeAllData -
 * Clears all data stored in the MySQL store, effectively resetting it.
 *
 * @property {function(string, string | null): Promise<void>} storeUserData -
 * Stores the user data to the MySQL store for handling group events.
 *
 * @property {function(string): Promise<string[]>} getMessageLabels -
 * Retrieves all labels associated with a specific message.
 *
 * @property {function(): Promise<proto.IWebMessageInfo[]> } getRecentStatusUpdates -
 * Retrieves all status updates sent by user in 24hrs.
 *
 * @property {function(string): Promise<proto.IWebMessageInfo>} mostRecentMessage -
 * Fetches the most recent message for a given chat from the MySQL store.
 *
 * @property {function(string, WASocket | undefined): Promise<string | null | undefined>} fetchImageUrl -
 * Fetches the profile image URL for a contact from the MySQL store, optionally using a WASocket connection for dynamic retrieval.
 *
 * @property {function(string, WASocket | undefined): Promise<GroupMetadata | null>} fetchGroupMetadata -
 * Retrieves group metadata for a specific group using the MySQL store or a WASocket connection.
 *
 * @property {function(WASocket | undefined): Promise<GroupMetadataResult>} fetchAllGroupsMetadata -
 * Retrieves group metadata for all user groups using a WASocket connection.
 *
 * @property {function(WAMessageKey): Promise<proto.IUserReceipt[] | null | undefined>} fetchMessageReceipts -
 * Retrieves delivery and read receipts for a specific message from the MySQL store.
 *
 * @property {function(): any} toJSON -
 * Serializes the store data into JSON format, suitable for exporting and backup.
 *
 * @property {function(any): void} fromJSON -
 * Loads store data from a JSON object, effectively restoring a previous state.
 *
 * @property {function(any): void} clearGroupsData -
 * Clearing all groups data for instance_id from MySQL db
 */

/**
 * Creates a MySQL-based store for managing Baileys  messages, groups, and contacts persistently.
 *
 * @param {string} instance_id - A unique identifier for the Baileys instance.
 * @param {Pool} pool - The MySQL connection pool used to interact with the database.
 * @param {pino.Logger} [logger] - Optional logger instance to log information and errors.
 *
 * @returns {makeMySQLStoreFunc} - Returns an object that implements the MySQL-based store with various data management functions.
 */
export function makeMySQLStore(
  instance_id: string,
  pool: Pool,
  skippedGroups: string[],
  logger?: pino.Logger
): makeMySQLStoreFunc {
  const state: ConnectionState | null = null;
  const cache = new LRUCache<string, any>({ max: 10000 });
  const writeQueue: Array<() => Promise<void>> = [];
  let isWriting = false;

  if (!pool) throw new Error("No MySQL connection pool provided");

  const log = logger || pino({ level: "info" });

  const createTablesIfNotExist = async () => {
    const queries = [
      // `CREATE TABLE IF NOT EXISTS baileys_store (
      //     instance_id VARCHAR(255) NOT NULL,
      //     \`key\` VARCHAR(255) NOT NULL,
      //     \`value\` JSON,
      //     PRIMARY KEY (instance_id, \`key\`)
      // ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,

      // `CREATE TABLE IF NOT EXISTS message_status (
      //     instance_id VARCHAR(255) NOT NULL,
      //     jid VARCHAR(255) NOT NULL,
      //     message_id VARCHAR(255) NOT NULL,
      //     from_me BOOLEAN DEFAULT FALSE,
      //     is_read BOOLEAN DEFAULT FALSE,
      //     PRIMARY KEY (instance_id, jid, message_id)
      // ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,

      `CREATE TABLE IF NOT EXISTS status_updates (
          id INT AUTO_INCREMENT PRIMARY KEY,
          instance_id VARCHAR(255) NOT NULL,
          message_type VARCHAR(50) NOT NULL,
          status_id VARCHAR(255) NOT NULL,
          post_date DATETIME NOT NULL,
          view_count INT DEFAULT 0,
          status_message JSON,
          UNIQUE(instance_id, status_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,

      `CREATE TABLE IF NOT EXISTS messages (
          id INT AUTO_INCREMENT PRIMARY KEY,
          instance_id VARCHAR(255) NOT NULL,
          message_id VARCHAR(255) NOT NULL,
          post_date DATETIME NOT NULL,
          message_data JSON,
          UNIQUE(instance_id, message_id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,

      `CREATE TABLE IF NOT EXISTS status_viewers (
          id INT AUTO_INCREMENT PRIMARY KEY,
          instance_id VARCHAR(255) NOT NULL,
          status_id VARCHAR(255) NOT NULL,
          viewer_jid VARCHAR(255) NOT NULL,
          view_date DATETIME NOT NULL,
          UNIQUE(instance_id, status_id, viewer_jid)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,

      `CREATE TABLE IF NOT EXISTS contacts (
          id INT AUTO_INCREMENT PRIMARY KEY,
          instance_id VARCHAR(255) NOT NULL,
          jid VARCHAR(255) NOT NULL,
          contact JSON,
          UNIQUE(instance_id, jid)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,

      `CREATE TABLE IF NOT EXISTS chats (
          id INT AUTO_INCREMENT PRIMARY KEY,
          instance_id VARCHAR(255) NOT NULL,
          jid VARCHAR(255) NOT NULL,
          chat JSON,
          UNIQUE(instance_id, jid)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,

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
          UNIQUE(instance_id, jid)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,

      `CREATE TABLE IF NOT EXISTS groups_status (
          id INT AUTO_INCREMENT PRIMARY KEY,
          instance_id VARCHAR(255) NOT NULL,
          status BOOLEAN DEFAULT FALSE,
          group_index INT DEFAULT 0,
          admin_index INT DEFAULT 0,
          UNIQUE(instance_id, group_index, admin_index)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,

      `CREATE TABLE IF NOT EXISTS users (
          id INT AUTO_INCREMENT PRIMARY KEY,
          instance_id VARCHAR(255) NOT NULL,
          username VARCHAR(255) NULL,
          jid VARCHAR(255) NULL,
          UNIQUE(instance_id, jid, username)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`
    ];

    for (const query of queries) {
      try {
        await pool.query(query);
        log.info(`Table created or already exists: ${query.split(" ")[5]}`);
      } catch (error) {
        log.error({ error, query }, "Failed to create table");
      }
    }
  };

  createTablesIfNotExist();

  const processWriteQueue = async () => {
    if (isWriting || writeQueue.length === 0) return;
    isWriting = true;
    while (writeQueue.length > 0) {
      const writeOp = writeQueue.shift();
      if (writeOp) {
        try {
          await writeOp();
        } catch (error) {
          log.error({ error }, "Error processing write queue");
        }
      }
    }
    isWriting = false;
  };

  const saveToMySQL = async (key: string, data: any) => {
    const writeOp = async () => {
      const conn = await pool.getConnection();
      try {
        await conn.beginTransaction();
        await conn.query(
          "INSERT INTO baileys_store (instance_id, `key`, `value`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `value` = ?",
          [instance_id, key, JSON.stringify(data), JSON.stringify(data)]
        );
        await conn.commit();
      } catch (error) {
        await conn.rollback();
        log.error({ error, key }, "Failed to save data to MySQL");
      } finally {
        conn.release();
      }
    };
    writeQueue.push(writeOp);
    if (!isWriting) {
      processWriteQueue();
    }
  };

  const saveStatusToMySQL = async (
    tableName: string,
    data: Record<string, any>
  ) => {
    const writeOp = async () => {
      const conn = await pool.getConnection();
      try {
        await conn.beginTransaction();

        const columns = Object.keys(data).join(", ");
        const placeholders = Object.keys(data)
          .map(() => "?")
          .join(", ");
        const updatePlaceholders = Object.keys(data)
          .map((key) => `\`${key}\` = VALUES(\`${key}\`)`)
          .join(", ");

        const query = `
        INSERT INTO ${tableName} (${columns}) 
        VALUES (${placeholders}) 
        ON DUPLICATE KEY UPDATE ${updatePlaceholders}`;
        const values = Object.values(data);

        await conn.query(query, values);
        await conn.commit();
      } catch (error) {
        await conn.rollback();
        log.error({ error, tableName, data }, "Failed to save status to MySQL");
      } finally {
        conn.release();
      }
    };

    writeQueue.push(writeOp);
    if (!isWriting) {
      processWriteQueue();
    }
  };

  const getUserData = async (): Promise<any | null> => {
    try {
      if (cache.has(instance_id)) {
        return cache.get(instance_id);
      }

      const sql = `SELECT * FROM users WHERE instance_id = ?`;
      const [rows] = await pool.query<RowDataPacket[]>(sql, [instance_id]);

      if (rows.length > 0) {
        const user_data = { username: rows[0].username, jid: rows[0].jid };

        cache.set(instance_id, user_data);
        return user_data;
      } else {
        log.warn({ instance_id }, "No user data found for instance_id");
      }
    } catch (error) {
      log.error({ error }, "Error fetching user data");
    }

    return null;
  };

  const isUserGroupAdmin = async (id: string): Promise<Boolean> => {
    try {
      const [rows] = await pool.query<RowDataPacket[]>(
        "SELECT is_admin admin_index FROM groups_metadata WHERE instance_id = ? AND jid = ?",
        [instance_id, id]
      );

      if (rows.length > 0) {
        return rows[0].is_admin === 1;
      }
    } catch (error) {
      log.error({ error }, "Error checking if user is group admin");
    }

    return false;
  };

  const isUserAdminOrSuperAdmin = async (participants: GroupParticipant[]) => {
    const { jid } = await getUserData();
    for (const participant of participants) {
      if (participant.id === jid) {
        if (
          participant.admin === "superadmin" ||
          participant.admin === "admin"
        ) {
          return true;
        }
      }
    }
    return false;
  };

  const getData = async (key: string): Promise<any> => {
    if (cache.has(key)) return cache.get(key);

    try {
      const [rows] = await pool.query<RowDataPacket[]>(
        "SELECT `value` FROM baileys_store WHERE instance_id = ? AND `key` = ?",
        [instance_id, key]
      );

      if (rows.length > 0) {
        try {
          let data;
          if (typeof rows[0].value === "object" && rows[0].value !== null) {
            data = rows[0].value;
          } else {
            data = JSON.parse(rows[0].value.toString());
          }
          cache.set(key, data);
          return data;
        } catch (parseError) {
          log.error(
            { error: parseError, key },
            "Failed to parse data from MySQL"
          );
          return null;
        }
      }
    } catch (error) {
      log.error({ error, key }, "Failed to get data from MySQL");
    }

    return null;
  };

  const hasGroups = async (): Promise<boolean> => {
    const [rows] = await pool.query<RowDataPacket[]>(
      "SELECT status FROM groups_status WHERE instance_id = ?",
      [instance_id]
    );

    return rows.length > 0 && rows[0].status === 1;
  };

  const setData = async (key: string, data: any): Promise<void> => {
    cache.set(key, data);
    await saveToMySQL(key, data);
  };

  const bind = async (ev: BaileysEventEmitter) => {
    ev.on("connection.update", async (update) => {
      Object.assign(state || {}, update);
      await setData(`connection.update`, update);
    });

    ev.on(
      "messaging-history.set",
      async (data: {
        chats: Chat[];
        contacts: Contact[];
        messages: WAMessage[];
        isLatest?: boolean;
        progress?: number | null;
        syncType?: proto.HistorySync.HistorySyncType;
        peerDataRequestSessionId?: string | null;
      }) => {
        const { chats, contacts, messages, isLatest } = data;

        const filteredChats = chats
          .filter((chat) => isJidUser(chat.id))
          .map((chat) => {
            if (
              chat.messages?.some(
                (m) => !m.message?.message && m.message?.messageStubType
              )
            ) {
              return undefined;
            }
            return chat;
          })
          .filter(Boolean) as Chat[];
        const filteredContacts = contacts.filter((contact) =>
          isJidUser(contact.id)
        );

        if (filteredChats.length > 0) {
          try {
            const chatRows = filteredChats.map((chat) => [
              instance_id,
              chat.id,
              JSON.stringify(chat)
            ]);

            const sql = `
              INSERT INTO chats (instance_id, jid, chat)
              VALUES ?
              ON DUPLICATE KEY UPDATE chat = VALUES(chat)
            `;

            const batchSize = 500;
            for (let i = 0; i < chatRows.length; i += batchSize) {
              const batch = chatRows.slice(i, i + batchSize);
              await customQuery(sql, [batch]);
            }
          } catch (error) {
            log.error({ error, instance_id }, "Failed to upsert chats");
          }
        }

        if (filteredContacts.length > 0) {
          try {
            const sql = `
              INSERT INTO contacts (instance_id, jid, contact)
              VALUES ?
              ON DUPLICATE KEY UPDATE
                contact = JSON_SET(VALUES(contact), '$.name', IFNULL(
                  JSON_UNQUOTE(JSON_EXTRACT(contacts.contact, '$.name')),
                  JSON_UNQUOTE(JSON_EXTRACT(VALUES(contact), '$.name'))
                ))
            `;

            const contactRows = filteredContacts.map((contact) => [
              instance_id,
              contact.id,
              JSON.stringify(contact)
            ]);

            const batchSize = 500;
            for (let i = 0; i < contactRows.length; i += batchSize) {
              const batch = contactRows.slice(i, i + batchSize);
              await customQuery(sql, [batch]);
            }
          } catch (error) {
            log.error({ error, instance_id }, "Failed to upsert contacts");
          }
        }
      }
    );

    ev.on("chats.upsert", async (chats: Chat[]) => {
      for (const chat of chats) {
        try {
          await saveStatusToMySQL("chats", {
            instance_id,
            jid: chat.id,
            chat: JSON.stringify(chat)
          });
        } catch (error) {
          log.error({ error, chat }, "Failed to upsert chat");
        }
      }
    });

    ev.on("contacts.upsert", async (contacts: Contact[]) => {
      const sqlUpsert = `
        INSERT INTO contacts (instance_id, jid, contact)
        VALUES (?, ?, ?)
        ON DUPLICATE KEY UPDATE
          contact = JSON_SET(VALUES(contact), '$.name', IFNULL(
            JSON_UNQUOTE(JSON_EXTRACT(contacts.contact, '$.name')),
            JSON_UNQUOTE(JSON_EXTRACT(VALUES(contact), '$.name'))
          ));
      `;

      for (const contact of contacts) {
        try {
          if (!isJidUser(contact.id)) continue;

          await customQuery(sqlUpsert, [
            instance_id,
            contact.id,
            JSON.stringify(contact)
          ]);
        } catch (error) {
          log.error({ error, contact }, "Failed to upsert contact");
        }
      }
    });

    ev.on(
      "messages.upsert",
      async (update: { messages: WAMessage[]; type: string }) => {
        try {
          for (const message of update.messages) {
            if (message.key && message.key.id) {
              if (message.key.fromMe) {
                await saveStatusToMySQL("messages", {
                  instance_id,
                  message_id: message.key.id,
                  message_data: JSON.stringify(message),
                  post_date: new Date()
                    .toISOString()
                    .slice(0, 19)
                    .replace("T", " ")
                });

                if (
                  isJidStatusBroadcast(message.key.remoteJid as string) &&
                  !message.message?.reactionMessage
                ) {
                  const messageType = getMessageType(message);
                  await saveStatusToMySQL("status_updates", {
                    instance_id,
                    status_id: message.key.id,
                    status_message: JSON.stringify(message),
                    post_date: new Date()
                      .toISOString()
                      .slice(0, 19)
                      .replace("T", " "),
                    message_type: messageType
                  });
                }
              } else if (
                !message.key.fromMe &&
                isJidUser(message.key.remoteJid as string)
              ) {
                const chat = await getChatById(message.key.remoteJid as string);
                const contact = await getContactById(
                  message.key.remoteJid as string
                );

                if (
                  contact &&
                  message.pushName &&
                  (!contact?.notify || contact?.notify === "")
                ) {
                  try {
                    const updatedContact = {
                      ...contact,
                      notify: message.pushName
                    };

                    const sql = `UPDATE contacts SET contact = ? WHERE jid = ? AND instance_id = ?`;
                    const params = [
                      JSON.stringify(updatedContact),
                      message.key.remoteJid,
                      instance_id
                    ];

                    await customQuery(sql, params);
                  } catch (error) {
                    log.error(
                      { error },
                      "Failed to update contact notify field"
                    );
                  }
                }

                if (!chat) {
                  ev.emit("chats.upsert", [
                    {
                      id: message.key.remoteJid as string,
                      conversationTimestamp: toNumber(message.messageTimestamp),
                      unreadCount: 1
                    }
                  ]);
                }

                if (!contact) {
                  ev.emit("contacts.upsert", [
                    {
                      id: message.key.remoteJid as string,
                      notify: message.pushName || ""
                    }
                  ]);
                }
              }
            }
          }
        } catch (error) {
          log.error(
            { error, key: { instanceId: instance_id } },
            "Failed to handle message upserts"
          );
        }
      }
    );

    ev.on(
      "message-receipt.update",
      async (updates: { key: proto.IMessageKey }[]) => {
        try {
          for (const update of updates) {
            if (
              update.key.id &&
              update.key.fromMe &&
              isJidStatusBroadcast(update.key.remoteJid as string)
            ) {
              await saveStatusToMySQL("status_viewers", {
                instance_id,
                status_id: update.key.id,
                viewer_jid: update.key.participant,
                view_date: new Date()
                  .toISOString()
                  .slice(0, 19)
                  .replace("T", " ")
              });

              const checkSql = `
                  SELECT COUNT(*) AS count FROM status_viewers
                  WHERE status_id = ? AND viewer_jid = ?
                `;

              const result = await customQuery(checkSql, [
                update.key.id,
                update.key.participant
              ]);

              if (result[0].viewer_count === 0) {
                const updateSql = `
                  UPDATE status_updates
                  SET view_count = view_count + 1
                  WHERE status_id = ?
                `;
                await customQuery(updateSql, [update.key.id]);
              }
            }
          }
        } catch (error) {
          log.error(
            { error, key: { instanceId: instance_id } },
            "Failed to handle message-reciept updates"
          );
        }
      }
    );

    ev.on("groups.update", async (updates: Partial<GroupMetadata>[]) => {
      const active = await hasGroups();
      if (active) {
        for (const update of updates) {
          if (update.id) {
            if (update.subject) {
              const metadata = await getGroupByJid(update.id);
              const subject = update.subject;

              if (metadata) {
                metadata.subject = subject;
                const metadataJson = JSON.stringify(metadata);

                const insertSql = `
                  INSERT INTO groups_metadata (instance_id, jid, subject, metadata)
                  VALUES (?, ?, ?, ?)
                  ON DUPLICATE KEY UPDATE subject = ?, metadata = ?`;

                await customQuery(insertSql, [
                  instance_id,
                  update.id,
                  subject,
                  metadataJson,
                  subject,
                  metadataJson
                ]);

                log.info({ groupId: update.id }, "Group subject updated");
              } else {
                log.warn({ groupId: update.id }, "No metadata found for group");
              }
            }
          }
        }
      }
    });

    ev.on("groups.upsert", async (groupMetadata: GroupMetadata[]) => {
      const active = await hasGroups();
      if (active) {
        try {
          for (const group of groupMetadata) {
            const {
              id,
              subject,
              announce,
              isCommunity,
              participants,
              isCommunityAnnounce
            } = group;

            const metadata = await getGroupByJid(id);

            if (metadata) continue;

            const sql = `SELECT group_index, admin_index FROM groups_status WHERE instance_id = ?`;
            const statusRows = await customQuery(sql, [instance_id]);

            if (statusRows.length > 0) {
              let { group_index, admin_index } = statusRows[0];
              const admin = await isUserAdminOrSuperAdmin(participants);

              if (
                (skippedGroups.includes(id) && !admin) ||
                (isCommunity && !announce && !isCommunityAnnounce) ||
                (announce && !isCommunity && isCommunityAnnounce && !admin)
              ) {
                continue;
              }

              group_index = group_index + 1;

              admin_index = admin ? admin_index + 1 : admin_index;
              const name = subject || `unnamed Group ${group_index}`;

              const metadataJson = JSON.stringify(group);

              const updateSql = `
                UPDATE groups_status 
                SET 
                  group_index = group_index + 1${
                    admin ? ", admin_index = admin_index + 1" : ""
                  }
                WHERE instance_id = ?`;

              await customQuery(updateSql, [instance_id]);

              const insertSql = `
                INSERT INTO groups_metadata (instance_id, jid, subject, is_admin, group_index, admin_index, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE 
                  subject = ?, 
                  is_admin = ?, 
                  group_index = ?, 
                  admin_index = ?, 
                  metadata = ?`;

              await customQuery(insertSql, [
                instance_id,
                id,
                name,
                admin,
                group_index,
                admin_index,
                metadataJson,
                name,
                admin,
                group_index,
                admin_index,
                metadataJson
              ]);

              log.info({ groupId: id }, "Group upserted");
            }
          }
        } catch (error) {
          log.error({ error }, "Failed to handle group upserts");
        }
      }
    });

    ev.on("group-participants.update", async ({ id, participants, action }) => {
      const active = await hasGroups();
      if (active) {
        const { jid } = await getUserData();

        const is_group_admin = await isUserGroupAdmin(id);

        if (
          is_group_admin ||
          (participants.includes(jid) && action === "promote")
        ) {
          const metadata = await getGroupByJid(id);

          if (metadata) {
            let is_admin = is_group_admin;
            let admin_promotion = false;
            let currentAdminIndex = 0;
            let participating = true;

            switch (action) {
              case "add":
                metadata.participants.push(
                  ...participants.map((id) => ({
                    id,
                    isAdmin: false,
                    isSuperAdmin: false
                  }))
                );
                break;
              case "promote":
              case "demote":
                if (!is_group_admin && action === "promote")
                  admin_promotion = true;
                for (const participant of metadata.participants) {
                  if (participants.includes(participant.id)) {
                    participant.isAdmin = action === "promote";
                  }
                }
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
              default:
                log.warn(
                  { action },
                  "Unknown action in group-participants.update"
                );
                break;
            }

            if (admin_promotion) {
              const adminIndexQuery = `SELECT admin_index FROM groups_status WHERE instance_id = ?`;
              const adminIndexResult = await customQuery(adminIndexQuery, [
                instance_id
              ]);
              const admin_index = adminIndexResult?.[0]?.admin_index ?? 0;

              if (admin_index > 0) {
                currentAdminIndex = admin_index + 1;

                const updateAdminIndexSql = `
                  UPDATE groups_status
                  SET admin_index = admin_index + 1
                  WHERE instance_id = ?
                `;
                await customQuery(updateAdminIndexSql, [instance_id]);

                log.info(
                  { groupId: id, admin_index: currentAdminIndex },
                  "Admin index updated due to promotion"
                );
              }
            }

            const metadataJson = JSON.stringify(metadata);

            const insertSql = `
              INSERT INTO groups_metadata (instance_id, jid, metadata, is_admin, participating, admin_index)
              VALUES (?, ?, ?, ?, ?, ?)
              ON DUPLICATE KEY UPDATE metadata = ?, is_admin = ?, participating = ?, admin_index = ?
            `;

            await customQuery(insertSql, [
              instance_id,
              id,
              metadataJson,
              is_admin,
              participating,
              currentAdminIndex,
              metadataJson,
              is_admin,
              participating,
              currentAdminIndex
            ]);

            log.info(
              { groupId: id },
              `Group participants updated for action: ${action}`
            );
          } else {
            log.warn(
              { groupId: id },
              "No metadata found for group, skipping participant update"
            );
          }
        }
      }
    });
  };

  const getMessageType = (message: proto.IWebMessageInfo): string => {
    const messageType = Object.keys(message.message || {})[0];
    return messageTypeMap[messageType] || "unknown";
  };

  const getRecentStatusUpdates = async (): Promise<proto.IWebMessageInfo[]> => {
    const messages: proto.IWebMessageInfo[] = [];
    try {
      const [rows] = await pool.query<RowDataPacket[]>(
        `SELECT status_message, view_count, message_type, post_date FROM status_updates
       WHERE instance_id = ?
         AND post_date >= NOW() - INTERVAL 24 HOUR`,
        [instance_id]
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
          log.error(
            { error: parseError },
            "Failed to parse status message data"
          );
        }
      }
    } catch (error) {
      log.error({ error, instance_id }, "Error fetching recent status updates");
      throw error;
    }
    return messages;
  };

  const getMessageLabels = async (messageId: string): Promise<string[]> => {
    const [rows] = await pool.query<RowDataPacket[]>(
      "SELECT `value` FROM baileys_store WHERE instance_id = ? AND `key` LIKE ?",
      [instance_id, `label-association-%-${messageId}`]
    );
    return rows
      .map((row) => {
        try {
          const association =
            typeof row.value === "object"
              ? row.value
              : JSON.parse(row.value.toString());
          return association.labelId;
        } catch (parseError) {
          log.error({ error: parseError }, "Failed to parse label association");
          return null;
        }
      })
      .filter(Boolean);
  };

  const mostRecentMessage = async (
    jid: string
  ): Promise<proto.IWebMessageInfo> => {
    const [rows] = await pool.query<RowDataPacket[]>(
      "SELECT `value` FROM baileys_store WHERE instance_id = ? AND `key` LIKE ? ORDER BY `key` DESC LIMIT 1",
      [instance_id, `messages-${jid}-%`]
    );
    if (rows.length > 0) {
      try {
        return typeof rows[0].value === "object"
          ? rows[0].value
          : JSON.parse(rows[0].value.toString());
      } catch (parseError) {
        log.error({ error: parseError }, "Failed to parse most recent message");
        throw parseError;
      }
    }
    throw new Error("No messages found for the given JID");
  };

  const fetchImageUrl = async (
    jid: string,
    sock: WASocket | undefined
  ): Promise<string | null | undefined> => {
    if (!sock) return undefined;
    try {
      const profilePictureUrl = await sock.profilePictureUrl(jid);
      return profilePictureUrl;
    } catch (error) {
      log.error({ error, jid }, "Failed to fetch image URL");
      return null;
    }
  };

  const fetchAllGroupsMetadata = async (
    sock: WASocket | undefined
  ): Promise<GroupMetadataResult> => {
    const inDB = await hasGroups();

    if (inDB) {
      try {
        const allGroupsQuery = `
          SELECT jid as id, subject, group_index AS groupIndex
          FROM groups_metadata
          WHERE instance_id = ?
          ORDER BY group_index ASC
        `;

        const allGroupsResult = (await customQuery(allGroupsQuery, [
          instance_id
        ])) as [{ id: string; subject: string; groupIndex: number }[]];

        const adminGroupsQuery = `
          SELECT jid as id, subject, admin_index AS adminIndex, 
                JSON_EXTRACT(metadata, '$.participants') AS participants
          FROM groups_metadata
          WHERE instance_id = ? AND is_admin = 1
          ORDER BY admin_index ASC
        `;

        const adminGroupsResult = (await customQuery(adminGroupsQuery, [
          instance_id
        ])) as [
          {
            id: string;
            subject: string;
            adminIndex: number;
            participants: string;
          }[]
        ];

        const allGroups = allGroupsResult.map((group: any) => ({
          id: group.id,
          subject: group.subject,
          groupIndex: group.groupIndex
        }));

        const adminGroups = adminGroupsResult.map((group: any) => ({
          id: group.id,
          subject: group.subject,
          adminIndex: group.adminIndex,
          participants:
            group.participants.map((participant: any) => participant.id) || []
        }));

        return {
          allGroups,
          adminGroups
        };
      } catch (error) {
        log.error(
          { error },
          "Failed to fetch groups metadata from the database"
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

        const excludeIds = Array.isArray(skippedGroups)
          ? skippedGroups
          : [skippedGroups];

        const groupMetadata: GroupMetadataEntry[] = [];

        const allGroups: {
          id: string;
          subject: string;
          groupIndex: number;
        }[] = [];
        const adminGroups: {
          id: string;
          subject: string;
          participants: string[];
          adminIndex: number;
        }[] = [];

        let groupIndex = 0;
        let adminIndex = 0;

        for (const [id, metadata] of sortedGroups) {
          const {
            subject,
            announce,
            isCommunity,
            participants,
            isCommunityAnnounce
          } = metadata;

          const admin = await isUserAdminOrSuperAdmin(participants);

          if (
            (skippedGroups.includes(id) && !admin) ||
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
            const participantIds = participants.map((p) => p.id);
            adminGroups.push({
              id,
              subject: name,
              participants: participantIds,
              adminIndex
            });
          }

          const groupData = {
            id,
            metadata,
            groupIndex,
            subject: name,
            isAdmin: admin,
            adminIndex: admin ? adminIndex : 0
          };

          groupMetadata.push(groupData);
        }

        const insertMetadataSql = `
          INSERT INTO groups_metadata (instance_id, jid, subject, is_admin, group_index, admin_index, metadata)
          VALUES (?, ?, ?, ?, ?, ?, ?)
          ON DUPLICATE KEY UPDATE is_admin = ?, metadata = ?
        `;

        for (const group of groupMetadata) {
          const metadataJson = JSON.stringify(group.metadata);
          const { id: jid, subject, isAdmin, groupIndex, adminIndex } = group;
          await customQuery(insertMetadataSql, [
            instance_id,
            jid,
            subject,
            isAdmin,
            groupIndex,
            adminIndex,
            metadataJson,
            isAdmin,
            metadataJson
          ]);
        }

        const updateGroupsStatusSql = `
          INSERT INTO groups_status (instance_id, status, group_index, admin_index)
          VALUES (?, ?, ?, ?)
          ON DUPLICATE KEY UPDATE
          group_index = group_index + VALUES(group_index),
          admin_index = admin_index + VALUES(admin_index)`;
        await pool.query(updateGroupsStatusSql, [
          instance_id,
          true,
          groupIndex,
          adminIndex
        ]);

        log.info(
          {
            instance_id,
            totalGroups: groupIndex,
            totalAdmins: adminIndex
          },
          "Groups metadata processed and stored successfully"
        );

        return { allGroups, adminGroups };
      } catch (error) {
        log.error({ error }, "Failed to fetch and process group metadata");
        throw error;
      }
    }
  };

  const fetchGroupMetadata = async (
    jid: string,
    sock: WASocket | undefined
  ): Promise<GroupMetadata | null> => {
    if (!sock) throw new Error("WASocket is undefined");

    try {
      const sql = `SELECT metadata FROM groups_metadata WHERE instance_id = ? AND jid = ?`;
      const [rows] = await pool.query<RowDataPacket[]>(sql, [instance_id, jid]);

      if (rows.length > 0) {
        const metadata: GroupMetadata = rows[0].metadata;
        return metadata;
      }

      log.info(
        { key: { jid } },
        "Group not found in database, fetching from WASocket"
      );

      const metadata = await sock.groupMetadata(jid);
      if (!metadata) {
        log.error(
          { key: { jid } },
          "Failed to fetch group metadata from WASocket"
        );
        return null;
      }

      const insertSql = `INSERT INTO groups_metadata (instance_id, jid, metadata) VALUES (?, ?, ?)`;
      await pool.query(insertSql, [instance_id, jid, JSON.stringify(metadata)]);

      return metadata;
    } catch (error) {
      log.error({ error, jid }, "Failed to fetch group metadata");
      throw error;
    }
  };

  const clearGroupsData = async (): Promise<void> => {
    try {
      const deleteGroupsSql = `
      DELETE FROM groups_metadata
      WHERE instance_id = ?`;

      await customQuery(deleteGroupsSql, [instance_id]);

      const deleteStatusSql = `
      DELETE FROM groups_status
      WHERE instance_id = ?`;

      await customQuery(deleteStatusSql, [instance_id]);

      log.info({ instance_id }, "Groups data cleared successfully");
    } catch (error) {
      log.error({ error, instance_id }, "Failed to clear groups data");
      throw error;
    }
  };

  // const demoteUser = async (jid: string): Promise<void> => {
  //   try {
  //     const fetchGroupSql = `
  //     SELECT is_admin
  //     FROM groups_metadata
  //     WHERE instance_id = ? AND jid = ?`;
  //     const groupRows = await customQuery(fetchGroupSql, [instance_id, jid]);

  //     if (groupRows.length === 0) {
  //       log.warn({ jid }, "Group not found in metadata, skipping demotion");
  //       return;
  //     }

  //     const { is_admin: isAdmin } = groupRows[0];

  //     if (!isAdmin) {
  //       log.info({ jid }, "User is already not an admin, skipping");
  //       return;
  //     }

  //     const updateIndexSql = `
  //     UPDATE groups_metadata
  //     SET is_admin = FALSE
  //     WHERE instance_id = ? AND jid = ?`;
  //     await customQuery(updateIndexSql, [instance_id, jid]);

  //     log.info({ jid }, "User demoted successfully");
  //   } catch (error) {
  //     log.error({ error, jid }, "Failed to demote user in group");
  //     throw error;
  //   }
  // };

  // const promoteUser = async (jid: string): Promise<void> => {
  //   try {
  //     const fetchGroupSql = `
  //     SELECT admin_index, participating
  //     FROM groups_metadata
  //     WHERE instance_id = ? AND jid = ?`;
  //     const groupRows = await customQuery(fetchGroupSql, [instance_id, jid]);

  //     if (groupRows.length === 0) {
  //       log.warn({ jid }, "Group not found in metadata, skipping promotion");
  //       return;
  //     }

  //     const { admin_index, participating } = groupRows[0];

  //     if (!participating) {
  //       log.warn(
  //         { jid },
  //         "User is not participating in the group, skipping promotion"
  //       );
  //       return;
  //     }

  //     if (admin_index > 0) {
  //       log.info({ jid }, "User is already an admin, skipping");
  //       return;
  //     }

  //     const fetchStatusSql = `
  //     SELECT admin_index
  //     FROM groups_status
  //     WHERE instance_id = ?`;
  //     const statusRows = await customQuery(fetchStatusSql, [instance_id]);
  //     const currentAdminIndex =
  //       statusRows.length > 0 ? statusRows[0].admin_index : null;

  //     if (currentAdminIndex === null) {
  //       log.error({ jid }, "No admin index found in status table");
  //       return;
  //     }

  //     const updateStatusSql = `
  //     UPDATE groups_status
  //     SET admin_index = admin_index + 1
  //     WHERE instance_id = ?`;
  //     await customQuery(updateStatusSql, [instance_id]);

  //     const updateMetadataSql = `
  //     UPDATE groups_metadata
  //     SET admin_index = ?, is_admin = TRUE
  //     WHERE instance_id = ? AND jid = ?`;
  //     await customQuery(updateMetadataSql, [
  //       currentAdminIndex + 1,
  //       instance_id,
  //       jid
  //     ]);

  //     log.info({ jid }, "Promoted user successfully");
  //   } catch (error) {
  //     log.error({ error, jid }, "Failed to promote user in group");
  //     throw error;
  //   }
  // };

  // const removeGroup = async (jid: string): Promise<void> => {
  //   try {
  //     const fetchGroupSql = `
  //     SELECT id
  //     FROM groups_metadata
  //     WHERE instance_id = ? AND jid = ?`;
  //     const groupRows = await customQuery(fetchGroupSql, [instance_id, jid]);

  //     if (groupRows.length === 0) {
  //       log.warn({ jid }, "Group not found in metadata, skipping removal");
  //       return;
  //     }

  //     const deleteMetadataSql = `
  //     UPDATE groups_metadata
  //     SET metadata = NULL,
  //         participating = FALSE,
  //         is_admin = FALSE
  //     WHERE instance_id = ? AND jid = ?`;
  //     await customQuery(deleteMetadataSql, [instance_id, jid]);

  //     log.info({ jid }, "Group removed successfully; metadata cleared");
  //   } catch (error) {
  //     log.error({ error, jid }, "Failed to remove group");
  //     throw error;
  //   }
  // };

  const fetchMessageReceipts = async ({
    remoteJid,
    id
  }: WAMessageKey): Promise<proto.IUserReceipt[] | null | undefined> => {
    const message = await loadMessage(id!);
    return message?.userReceipt;
  };

  const toJSON = () => {
    return {
      chats: getData("chats"),
      contacts: getData("contacts"),
      messages: getData("messages"),
      labels: getData("labels"),
      labelAssociations: getData("labelAssociations")
    };
  };

  const fromJSON = (json: any) => {
    const { chats, contacts, messages, labels, labelAssociations } = json;
    setData("chats", chats);
    setData("contacts", contacts);
    setData("messages", messages);
    setData("labels", labels);
    setData("labelAssociations", labelAssociations);
  };

  const loadMessages = async (
    jid: string,
    count: number
  ): Promise<proto.IWebMessageInfo[]> => {
    const messages: proto.IWebMessageInfo[] = [];
    const [rows] = await pool.query<RowDataPacket[]>(
      "SELECT `value` FROM baileys_store WHERE instance_id = ? AND `key` LIKE ? ORDER BY `key` DESC LIMIT ?",
      [instance_id, `messages-${jid}-%`, count]
    );
    for (const row of rows) {
      try {
        const message =
          typeof row.value === "object"
            ? row.value
            : JSON.parse(row.value.toString());
        messages.push(message);
      } catch (parseError) {
        log.error({ error: parseError }, "Failed to parse message data");
      }
    }
    return messages;
  };

  const loadMessage = async (
    id: string
  ): Promise<proto.IWebMessageInfo | undefined> => {
    if (!id) {
      throw new Error("Invalid id");
    }

    const messageSql =
      "SELECT message_data from messages WHERE instance_id = ? and message_id = ?";
    const message_rows = await customQuery(messageSql, [instance_id, id]);

    if (message_rows.length > 0) {
      const message = message_rows[0].message_data;
      return message;
    }

    return undefined;
  };

  const loadAllGroupMetadata = async (): Promise<GroupMetadata[]> => {
    try {
      const [rows] = await pool.query<RowDataPacket[]>(
        "SELECT `value` FROM baileys_store WHERE instance_id = ? AND `key` LIKE ?",
        [instance_id, "group-%"]
      );
      return rows
        .map((row) => {
          try {
            return typeof row.value === "object"
              ? row.value
              : JSON.parse(row.value.toString());
          } catch (parseError) {
            log.error({ error: parseError }, "Failed to parse group metadata");
            return null;
          }
        })
        .filter(Boolean);
    } catch (error) {
      log.error({ error }, "Failed to load all group metadata");
      return [];
    }
  };

  const loadGroupMetadataByJid = async (
    jid: string
  ): Promise<GroupMetadata | undefined> => {
    return await getData(`group-${jid}`);
  };

  const customQuery = async (query: string, params?: any[]): Promise<any> => {
    try {
      const [result] = await pool.query(query, params);
      return result;
    } catch (error) {
      log.error({ error, query }, "Failed to execute custom query");
      throw error;
    }
  };

  const getAllChats = async (): Promise<Chat[]> => {
    try {
      const sql = `SELECT chat FROM chats WHERE instance_id = ?`;
      const [rows] = await pool.query<RowDataPacket[]>(sql, [instance_id]);

      return rows.map((row) => {
        const chat =
          typeof row.chat === "string" ? JSON.parse(row.chat) : row.chat;
        return chat;
      });
    } catch (error) {
      log.error(
        { error, key: { instanceId: instance_id } },
        "Failed to retrieve all chats"
      );
      throw error;
    }
  };

  const getAllContacts = async (): Promise<Contact[]> => {
    try {
      const sql = `SELECT contact FROM contacts WHERE instance_id = ?`;
      const [rows] = await pool.query<RowDataPacket[]>(sql, [instance_id]);

      return rows.map((row) => {
        const contact =
          typeof row.contact === "string"
            ? JSON.parse(row.contact)
            : row.contact;
        return contact;
      });
    } catch (error) {
      log.error(
        { error, key: { instanceId: instance_id } },
        "Failed to retrieve contacts"
      );
      throw error;
    }
  };

  const getAllSavedContacts = async (): Promise<Contact[]> => {
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

      const [rows] = await customQuery(sql, [instance_id]);

      return rows;
    } catch (error) {
      log.error(
        { error, key: { instanceId: instance_id } },
        "Failed to retrieve saved contacts"
      );
      throw error;
    }
  };

  const getChatById = async (jid: string): Promise<Chat | undefined> => {
    try {
      const sql = `SELECT chat FROM chats WHERE jid = ?`;
      const [rows] = await pool.query<RowDataPacket[]>(sql, [jid]);

      if (rows.length === 0) {
        log.error({ key: { jid } }, "No chat found with ID");
        return undefined;
      }

      const chat = rows[0].chat;
      return chat;
    } catch (error) {
      log.error({ error, key: { jid } }, "Failed to retrieve chat with ID");
      throw error;
    }
  };

  const getContactById = async (jid: string): Promise<Contact | undefined> => {
    try {
      const sql = `SELECT contact FROM contacts WHERE jid = ?`;
      const [rows] = await pool.query<RowDataPacket[]>(sql, [jid]);

      if (rows.length === 0) {
        log.error({ key: { jid } }, "No contact found with ID");
        return undefined;
      }

      const contact = rows[0].contact;
      return contact;
    } catch (error) {
      log.error({ error, key: { jid } }, "Failed to retrieve contact with ID");
      throw error;
    }
  };

  const getGroupByJid = async (
    jid: string
  ): Promise<GroupMetadata | undefined> => {
    const sql = `SELECT * FROM groups_metadata WHERE instance_id = ? AND jid = ?`;
    const [rows] = await pool.query<RowDataPacket[]>(sql, [instance_id, jid]);

    if (rows.length > 0) {
      try {
        const participating = rows[0].participating;
        const metadata: GroupMetadata = participating
          ? rows[0].metadata
          : undefined;
        return metadata;
      } catch (error) {
        log.error({ error, jid }, "Failed to parse group metadata");
        return undefined;
      }
    }

    return undefined;
  };

  const storeUserData = async (jid: string, username: string | null) => {
    try {
      const userSQL = `
      INSERT INTO users (instance_id, jid, username)
      VALUES (?, ?, ?)
      ON DUPLICATE KEY UPDATE username = VALUES(username)`;
      await customQuery(userSQL, [instance_id, jid, username]);
      log.info({ jid, username }, "User data stored/updated successfully");
    } catch (error) {
      log.error({ error, jid }, "Failed to insert/update user data");
    }
  };

  const updateMessageStatus = async (
    jid: string,
    id: string,
    status: "sent" | "delivered" | "read"
  ): Promise<void> => {
    try {
      const fromMe = status === "sent";
      const isRead = status === "read";
      await pool.query<ResultSetHeader>(
        "INSERT INTO message_status (instance_id, jid, message_id, from_me, is_read) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE from_me = ?, is_read = ?",
        [instance_id, jid, id, fromMe, isRead, fromMe, isRead]
      );
    } catch (error) {
      log.error({ error, jid, id, status }, "Failed to update message status");
    }
  };

  const removeAllData = async (): Promise<void> => {
    try {
      // await pool.query("DELETE FROM baileys_store WHERE instance_id = ?", [
      //   instance_id
      // ]);
      // await pool.query("DELETE FROM message_status WHERE instance_id = ?", [
      //   instance_id
      // ]);
      await pool.query("DELETE FROM chats WHERE instance_id = ?", [
        instance_id
      ]);
      await pool.query("DELETE FROM contacts WHERE instance_id = ?", [
        instance_id
      ]);
      await pool.query("DELETE FROM messages WHERE instance_id = ?", [
        instance_id
      ]);
      await pool.query("DELETE FROM users WHERE instance_id = ?", [
        instance_id
      ]);
      await pool.query("DELETE FROM groups_metadata WHERE instance_id = ?", [
        instance_id
      ]);
      await pool.query("DELETE FROM groups_status WHERE instance_id = ?", [
        instance_id
      ]);
      cache.clear();
      log.info({ instance_id }, "All data removed for instance");
    } catch (error) {
      log.error({ error, instance_id }, "Failed to remove all data");
      throw error;
    }
  };

  return {
    bind,
    state,
    toJSON,
    getData,
    setData,
    fromJSON,
    customQuery,
    getAllChats,
    getChatById,
    loadMessage,
    loadMessages,
    getGroupByJid,
    removeAllData,
    storeUserData,
    fetchImageUrl,
    getContactById,
    getAllContacts,
    clearGroupsData,
    getMessageLabels,
    mostRecentMessage,
    fetchGroupMetadata,
    updateMessageStatus,
    getAllSavedContacts,
    fetchMessageReceipts,
    loadAllGroupMetadata,
    loadGroupMetadataByJid,
    fetchAllGroupsMetadata,
    getRecentStatusUpdates
  };
}
