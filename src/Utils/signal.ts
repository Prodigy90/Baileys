import { chunk } from "lodash";
import { KEY_BUNDLE_TYPE } from "../Defaults";
import { SignalRepository } from "../Types";
import {
  AuthenticationCreds,
  AuthenticationState,
  KeyPair,
  SignalIdentity,
  SignalKeyStore,
  SignedKeyPair,
} from "../Types/Auth";
import {
  assertNodeErrorFree,
  BinaryNode,
  getBinaryNodeChild,
  getBinaryNodeChildBuffer,
  getBinaryNodeChildren,
  getBinaryNodeChildUInt,
  jidDecode,
  JidWithDevice,
  S_WHATSAPP_NET,
} from "../WABinary";
import { Curve, generateSignalPubKey } from "./crypto";
import { encodeBigEndian } from "./generics";
import * as os from "os";
import { Worker, isMainThread, parentPort, workerData } from "worker_threads";

interface WorkerData {
  nodesChunk: BinaryNode[];
  repository: SignalRepository;
}

const processNodesChunk = async (
  nodesChunk: BinaryNode[],
  repository: SignalRepository
): Promise<void> => {
  const extractKey = (key: BinaryNode) =>
    key
      ? {
          keyId: getBinaryNodeChildUInt(key, "id", 3)!,
          publicKey: generateSignalPubKey(
            getBinaryNodeChildBuffer(key, "value")!
          )!,
          signature: getBinaryNodeChildBuffer(key, "signature")!,
        }
      : undefined;

  await Promise.all(
    nodesChunk.map(async (node) => {
      const signedKey = getBinaryNodeChild(node, "skey")!;
      const key = getBinaryNodeChild(node, "key")!;
      const identity = getBinaryNodeChildBuffer(node, "identity")!;
      const jid = node.attrs.jid;
      const registrationId = getBinaryNodeChildUInt(node, "registration", 4);
      console.log(`Processing node: ${jid}`);
      try {
        await repository.injectE2ESession({
          jid,
          session: {
            registrationId: registrationId!,
            identityKey: generateSignalPubKey(identity),
            signedPreKey: extractKey(signedKey)!,
            preKey: extractKey(key)!,
          },
        });
        console.log(`Successfully processed node: ${jid}`);
      } catch (error) {
        console.error(`Error processing node: ${jid}`, error);
      }
    })
  );
};

if (!isMainThread) {
  const { nodesChunk, repository } = workerData as WorkerData;
  processNodesChunk(nodesChunk, repository)
    .then(() => {
      parentPort?.postMessage("done");
    })
    .catch((err) => {
      parentPort?.postMessage({ error: err.message });
    });
}

export const createSignalIdentity = (
  wid: string,
  accountSignatureKey: Uint8Array
): SignalIdentity => {
  return {
    identifier: { name: wid, deviceId: 0 },
    identifierKey: generateSignalPubKey(accountSignatureKey),
  };
};

export const getPreKeys = async (
  { get }: SignalKeyStore,
  min: number,
  limit: number
) => {
  const idList: string[] = [];
  for (let id = min; id < limit; id++) {
    idList.push(id.toString());
  }

  return get("pre-key", idList);
};

export const generateOrGetPreKeys = (
  creds: AuthenticationCreds,
  range: number
) => {
  const avaliable = creds.nextPreKeyId - creds.firstUnuploadedPreKeyId;
  const remaining = range - avaliable;
  const lastPreKeyId = creds.nextPreKeyId + remaining - 1;
  const newPreKeys: { [id: number]: KeyPair } = {};
  if (remaining > 0) {
    for (let i = creds.nextPreKeyId; i <= lastPreKeyId; i++) {
      newPreKeys[i] = Curve.generateKeyPair();
    }
  }

  return {
    newPreKeys,
    lastPreKeyId,
    preKeysRange: [creds.firstUnuploadedPreKeyId, range] as const,
  };
};

export const xmppSignedPreKey = (key: SignedKeyPair): BinaryNode => ({
  tag: "skey",
  attrs: {},
  content: [
    { tag: "id", attrs: {}, content: encodeBigEndian(key.keyId, 3) },
    { tag: "value", attrs: {}, content: key.keyPair.public },
    { tag: "signature", attrs: {}, content: key.signature },
  ],
});

export const xmppPreKey = (pair: KeyPair, id: number): BinaryNode => ({
  tag: "key",
  attrs: {},
  content: [
    { tag: "id", attrs: {}, content: encodeBigEndian(id, 3) },
    { tag: "value", attrs: {}, content: pair.public },
  ],
});

export const parseAndInjectE2ESessions = async (
  node: BinaryNode,
  repository: SignalRepository
) => {
  const extractKey = (key: BinaryNode) =>
    key
      ? {
          keyId: getBinaryNodeChildUInt(key, "id", 3)!,
          publicKey: generateSignalPubKey(
            getBinaryNodeChildBuffer(key, "value")!
          )!,
          signature: getBinaryNodeChildBuffer(key, "signature")!,
        }
      : undefined;

  const nodes = getBinaryNodeChildren(getBinaryNodeChild(node, "list"), "user");
  for (const node of nodes) {
    assertNodeErrorFree(node);
  }

  const chunkSize = 100;
  const chunks = Array.from(
    { length: Math.ceil(nodes.length / chunkSize) },
    (_, i) => nodes.slice(i * chunkSize, i * chunkSize + chunkSize)
  );
  console.log(
    new Date().toLocaleString(),
    "e2e nodes",
    nodes.length,
    chunks.length
  );

  if (nodes.length > 1000 && isMainThread) {
    const numCores = os.cpus().length; // Get the number of CPU cores
    const workerPromises = chunks
      .map((nodesChunk, index) => {
        return new Promise((resolve, reject) => {
          const worker = new Worker(__filename, {
            workerData: {
              nodesChunk,
              repository,
            },
          });
          worker.on("message", resolve);
          worker.on("error", reject);
          worker.on("exit", (code) => {
            if (code !== 0)
              reject(new Error(`Worker stopped with exit code ${code}`));
          });
        });
      })
      .slice(0, numCores); // Limit the number of workers to the number of CPU cores
    await Promise.all(workerPromises);
  } else {
    for (const nodesChunk of chunks) {
      await processNodesChunk(nodesChunk, repository);
    }
  }

  console.log(new Date().toLocaleString(), "e2e nodes done");
};

export const extractDeviceJids = (
  result: BinaryNode,
  myJid: string,
  excludeZeroDevices: boolean
) => {
  const { user: myUser, device: myDevice } = jidDecode(myJid)!;
  const extracted: JidWithDevice[] = [];
  for (const node of result.content as BinaryNode[]) {
    const list = getBinaryNodeChild(node, "list")?.content;
    if (list && Array.isArray(list)) {
      for (const item of list) {
        const { user } = jidDecode(item.attrs.jid)!;
        const devicesNode = getBinaryNodeChild(item, "devices");
        const deviceListNode = getBinaryNodeChild(devicesNode, "device-list");
        if (Array.isArray(deviceListNode?.content)) {
          for (const { tag, attrs } of deviceListNode!.content) {
            const device = +attrs.id;
            if (
              tag === "device" && // ensure the "device" tag
              (!excludeZeroDevices || device !== 0) && // if zero devices are not-excluded, or device is non zero
              (myUser !== user || myDevice !== device) && // either different user or if me user, not this device
              (device === 0 || !!attrs["key-index"]) // ensure that "key-index" is specified for "non-zero" devices, produces a bad req otherwise
            ) {
              extracted.push({ user, device });
            }
          }
        }
      }
    }
  }

  return extracted;
};

/**
 * get the next N keys for upload or processing
 * @param count number of pre-keys to get or generate
 */
export const getNextPreKeys = async (
  { creds, keys }: AuthenticationState,
  count: number
) => {
  const { newPreKeys, lastPreKeyId, preKeysRange } = generateOrGetPreKeys(
    creds,
    count
  );

  const update: Partial<AuthenticationCreds> = {
    nextPreKeyId: Math.max(lastPreKeyId + 1, creds.nextPreKeyId),
    firstUnuploadedPreKeyId: Math.max(
      creds.firstUnuploadedPreKeyId,
      lastPreKeyId + 1
    ),
  };

  await keys.set({ "pre-key": newPreKeys });

  const preKeys = await getPreKeys(
    keys,
    preKeysRange[0],
    preKeysRange[0] + preKeysRange[1]
  );

  return { update, preKeys };
};

export const getNextPreKeysNode = async (
  state: AuthenticationState,
  count: number
) => {
  const { creds } = state;
  const { update, preKeys } = await getNextPreKeys(state, count);

  const node: BinaryNode = {
    tag: "iq",
    attrs: {
      xmlns: "encrypt",
      type: "set",
      to: S_WHATSAPP_NET,
    },
    content: [
      {
        tag: "registration",
        attrs: {},
        content: encodeBigEndian(creds.registrationId),
      },
      { tag: "type", attrs: {}, content: KEY_BUNDLE_TYPE },
      { tag: "identity", attrs: {}, content: creds.signedIdentityKey.public },
      {
        tag: "list",
        attrs: {},
        content: Object.keys(preKeys).map((k) => xmppPreKey(preKeys[+k], +k)),
      },
      xmppSignedPreKey(creds.signedPreKey),
    ],
  };

  return { update, node };
};
