import { addHandler, handleMessage, download } from "libkmodule";
import type { ActiveQuery } from "libkmodule";
import PQueue from "p-queue";
import fetchRetry from "fetch-retry";
import { ipfsPath, ipnsPath } from "is-ipfs";
import { Buffer } from "buffer";
import {
  bufToStr,
  deriveRegistryEntryID,
  entryIDToSkylink,
} from "libskynet";
import { hashDataKey } from "@lumeweb/kernel-utils";

onmessage = handleMessage;

let blockingGatewayUpdate = Promise.resolve();

let activeGateways = [];

const fetch = fetchRetry(self.fetch);

const IPFS_FILE = "bafybeifx7yeb55armcsxwwitkymga5xf53dxiarykms3ygqic223w5sk3m";

const gatewayListName = "ipfs-gateways";
const gatewayListOwner = Buffer.from(
  "86c7421160eb5cb4a39495fc3e3ae25a60b330fff717e06aab978ad353722014",
  "hex"
);

addHandler("presentSeed", handlePresentSeed);
addHandler("refreshGatewayList", handleRefreshGatewayList);
addHandler("fetchIpfs", handleFetchIpfs);
addHandler("fetchIpns", handleFetchIpns);

async function handlePresentSeed(aq: ActiveQuery) {
  await refreshGatewayList();
  aq.respond();
}

async function handleRefreshGatewayList(aq: ActiveQuery) {
  await blockingGatewayUpdate;
  await refreshGatewayList();
  aq.respond();
}

async function handleFetchIpfs(aq: ActiveQuery) {
  const { hash = null } = aq.callerInput;
  const { headers = {} } = aq.callerInput;

  if (!hash) {
    aq.reject("hash missing");
    return;
  }

  if (!ipfsPath(`/ipfs/${hash}`)) {
    aq.reject("hash is invalid");
    return;
  }

  await blockingGatewayUpdate;

  let stream: ReadableStream<Uint8Array>;

  try {
    stream = await progressiveFetch(`/ipfs/${hash}`, headers);
  } catch (e) {
    aq.reject(e);
    return;
  }

  await processStream(aq, stream);
}

async function handleFetchIpns(aq: ActiveQuery) {
  const { hash = null } = aq.callerInput;
  const { headers = {} } = aq.callerInput;

  if (!hash) {
    aq.reject("hash missing");
    return;
  }

  if (!ipnsPath(`/ipns/${hash}`)) {
    aq.reject("hash is invalid");
    return;
  }

  await blockingGatewayUpdate;

  let stream: ReadableStream<Uint8Array>;

  try {
    stream = await progressiveFetch(`/ipns/${hash}`, headers);
  } catch (e) {
    aq.reject(e);
    return;
  }

  await processStream(aq, stream);
}

async function processStream(
  aq: ActiveQuery,
  stream: ReadableStream<Uint8Array>
) {
  const reader = stream.getReader();

  while (true) {
    let chunk;

    try {
      chunk = await reader.read();
      aq.sendUpdate(chunk.value);
      if (chunk.done) {
        aq.respond();
        break;
      }
    } catch (e) {
      aq.respond();
      break;
    }
  }
}

async function progressiveFetch(
  path: string,
  headers = {}
): Promise<ReadableStream<Uint8Array>> {
  const currentGateways = activeGateways.slice();

  let reader: ReadableStream<Uint8Array>;
  let lastErr: Error = new Error("fetch failed");

  for (const gateway of currentGateways) {
    let resp: Response;

    try {
      resp = await fetch(`${gateway}${path}`, { headers });
    } catch (e: any) {
      lastErr = e as Error;
      continue;
    }

    if (!resp.ok) {
      continue;
    }

    reader = resp.body;
    break;
  }

  if (!reader) {
    throw lastErr;
  }

  return reader;
}

async function refreshGatewayList() {
  let [fileData] = await download(
    entryIDToSkylink(
      deriveRegistryEntryID(gatewayListOwner, hashDataKey(gatewayListName))[0]
    )
  );
  let json = bufToStr(fileData)[0];

  let gatewayList = JSON.parse(json);

  let processResolve: any;
  blockingGatewayUpdate = new Promise((resolve) => {
    processResolve = resolve;
  });
  const queue = new PQueue({ concurrency: 10 });

  let latencies = [];

  gatewayList.forEach((item) => {
    queue.add(checkGatewayLatency(new URL(item), latencies));
  });

  await queue.onIdle();

  activeGateways = latencies
    .sort((a: any[], b: any[]) => {
      return a[0] - b[0];
    })
    .map((item: any[]) => item[1]);
  processResolve();
}

function checkGatewayLatency(server: URL, list: any[]) {
  return async () => {
    const start = Date.now();
    const controller = new AbortController();
    setTimeout(() => controller.abort(), 5000);

    let result: Response;
    const fullServer = `${server.protocol}//${server.hostname}`;
    try {
      result = await fetch(`${fullServer}/ipfs/${IPFS_FILE}`, {
        signal: controller.signal,
      });
    } catch {
      return;
    }

    if (result.ok) {
      const end = Date.now() - start;
      list.push([end, fullServer]);
    }
  };
}
