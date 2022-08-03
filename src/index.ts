import { addHandler, handleMessage, download, log } from "libkmodule";
import type { ActiveQuery } from "libkmodule";
import PQueue from "p-queue";
import fetchRetry from "fetch-retry";
import { ipfsPath, ipnsPath } from "is-ipfs";
import { Buffer } from "buffer";
import { bufToStr, deriveRegistryEntryID, entryIDToSkylink } from "libskynet";
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

const fetchRetryOn = (attempt, error, response) => {
  if (response?.type == "cors") {
    return false;
  }
  return !(error && !response);
};

addHandler("presentSeed", handlePresentSeed);
addHandler("refreshGatewayList", handleRefreshGatewayList);
addHandler("isIpfs", handleIsIpfs);
addHandler("fetchIpfs", handleFetchIpfs);
addHandler("isIpns", handleFetchIpns);
addHandler("fetchIpns", handleIsIpns);

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
  const { path = "" } = aq.callerInput;
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
  let stream: Response;

  try {
    stream = await progressiveFetch(`/ipfs/${hash}${path}`, headers);
  } catch (e) {
    aq.reject(e);
    return;
  }

  await processStream(aq, stream);
}

async function handleIsIpfs(aq: ActiveQuery) {
  const { hash = null } = aq.callerInput;
  const { path = "" } = aq.callerInput;
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

  let resp: Response;
  try {
    resp = await progressiveFetch(`/ipfs/${hash}${path}`, headers, true);
  } catch (e) {
    aq.reject(e);
    return;
  }

  aq.respond({ headers: Array.from(resp.headers), status: resp.status });
}

async function handleFetchIpns(aq: ActiveQuery) {
  const { hash = null } = aq.callerInput;
  const { path = "" } = aq.callerInput;
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

  let resp: Response;

  try {
    resp = await progressiveFetch(`/ipns/${hash}${path}`, headers);
  } catch (e) {
    aq.reject(e);
    return;
  }

  await processStream(aq, resp);
}

async function handleIsIpns(aq: ActiveQuery) {
  const { hash = null } = aq.callerInput;
  const { path = "" } = aq.callerInput;
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

  let resp: Response;

  try {
    resp = await progressiveFetch(`/ipns/${hash}${path}`, headers, true);
  } catch (e) {
    aq.reject(e);
    return;
  }

  aq.respond({ headers: Array.from(resp.headers), status: resp.status });
}

async function processStream(aq: ActiveQuery, response: Response) {
  const reader = response.body.getReader();

  let aqResp = {
    headers: Array.from(response.headers),
    status: response.status,
  };
  while (true) {
    let chunk;

    try {
      chunk = await reader.read();
      aq.sendUpdate(chunk.value);
      if (chunk.done) {
        aq.respond(aqResp);
        break;
      }
    } catch (e) {
      aq.respond(aqResp);
      break;
    }
  }
}

async function progressiveFetch(
  path: string,
  headers = {},
  head = false
): Promise<Response> {
  const currentGateways = activeGateways.slice();

  let reader: ReadableStream<Uint8Array>;
  let lastErr: Error = new Error("fetch failed");
  let resp: Response;

  let requests = [];
  let controllers: AbortController[] = [];
  for (const gateway of currentGateways) {
    try {
      const controller = new AbortController();
      controllers.push(controller);
      setTimeout(() => controller.abort(), 1000);
      requests.push(
        fetch(`${gateway}${path}`, {
          headers,
          retryOn: fetchRetryOn,
          signal: controller.signal,
          method: head ? "HEAD" : undefined,
        })
      );
    } catch (e: any) {
      lastErr = e as Error;
    }
  }

  resp = await Promise.any(requests);
  controllers.forEach((controller) => controller.abort());

  reader = resp?.body;

  if (!reader) {
    throw lastErr;
  }

  return resp;
}

async function refreshGatewayList() {
  let processResolve: any;
  blockingGatewayUpdate = new Promise((resolve) => {
    processResolve = resolve;
  });
  let [fileData] = await download(
    entryIDToSkylink(
      deriveRegistryEntryID(gatewayListOwner, hashDataKey(gatewayListName))[0]
    )
  );
  let json = bufToStr(fileData)[0];

  let gatewayList = JSON.parse(json);

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
    setTimeout(() => controller.abort(), 1000);

    let result: Response;
    const fullServer = `${server.protocol}//${server.hostname}`;
    try {
      result = await fetch(`${fullServer}/ipfs/${IPFS_FILE}`, {
        signal: controller.signal,
        retryOn: fetchRetryOn,
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
