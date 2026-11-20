import { createVideo, getProfileByExtID, getProfiles, getVideoByExtID, setPublicVideo } from "@qwru/db";
import { YoutubeProfile, DiscordMessageBody, YoutubeMessageBody } from "@qwru/types";
import { env, waitUntil } from "cloudflare:workers";
import { timingSafeEqual } from "crypto";
import { XMLParser } from "fast-xml-parser";

interface Env {
  DISCORD_QUEUE: Queue<DiscordMessageBody>;
}

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '@_',
})

const HUB_URL = "https://pubsubhubbub.appspot.com/subscribe";
const callbackUrl = "https://youtubehook.qwru0905.dev";

async function registerYoutubeChannel(profile: YoutubeProfile) {
  const channelId = profile.ext_id;
  const leastSeconds = 432000;

  const body = new URLSearchParams({
    "hub.mode": "subscribe",
    "hub.topic": `https://www.youtube.com/xml/feeds/videos.xml?channel_id=${channelId}`,
    "hub.callback": callbackUrl,
    "hub.verify": "async",
    "hub.lease_seconds": String(leastSeconds),
    "hub.secret": env.WEBHOOK_SECRET,
  });

  const res = await fetch(HUB_URL, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body,
  });

  if (res.status !== 202 && res.status !== 204) {
    throw new Error(`${channelId} register failed: ${res.status} ${await res.text()}`);
  }
}

async function registerAll() {
  const db = env.DB;
  const profiles = await getProfiles(db);

  for (const profile of profiles) {
    await registerYoutubeChannel(profile);
  }
}

export async function verifyHmac(body: string, header: string, secret: string) {
  if (!header.startsWith('sha1=')) return false;
  const key = await crypto.subtle.importKey(
    'raw', new TextEncoder().encode(secret),
    { name: 'HMAC', hash: 'SHA-1' }, false, ['sign']
  );
  const sig = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(body));
  const expected = 'sha1=' + [...new Uint8Array(sig)]
    .map(b => b.toString(16).padStart(2, '0')).join('');

  if (expected.length !== header.length) return false;
  return timingSafeEqual(new TextEncoder().encode(expected), new TextEncoder().encode(header));
}

export async function processYoutubeHook(feed: YtFeed) {
  const db = env.DB;
  const entry = feed.entry;
  if (!entry) return;
  const videoId = entry["yt:videoId"];
  const video = await getVideoByExtID(db, videoId);
  if (video) {
    if (!video.is_public) {
      setPublicVideo(db, video.id, true);
    }
    return;
  }
  const channelId = entry["yt:channelId"];
  const channel = await getProfileByExtID(db, channelId);
  if (!channel) {
    throw new Error("wtf: channel isnt exist (processYoutubeHook)");
  }
  const video2 = await createVideo(db, videoId, channel.code, entry.title, { is_public: true })
  if (!video2) {
    throw new Error(`why video doesnt created (processYoutubeHook)`);
  }

  await env.DISCORD_QUEUE.send({
    action: "youtube_hook",
    videoId: videoId,
  });
}

async function challenge(request: Request): Promise<Response> {
  const url = new URL(request.url);
  const hubMode = url.searchParams.get("hub.mode");
  const hubChallenge = url.searchParams.get("hub.challenge");

  if ((hubMode === "subscribe" || hubMode === "unsubscribe") && hubChallenge) {
    // 허브에서 보낸 챌린지 코드를 평문(text/plain) 그대로 200 상태코드로 반환해야 인증이 완료됩니다.
    return new Response(hubChallenge, {
      status: 200,
      headers: { "Content-Type": "text/plain" }
    });
  }
  return new Response("Missing WebSub verification parameters", { status: 400 });
}

async function post(request: Request) {
  const raw = await request.text();                       // raw body for signature checks
  const signature = request.headers.get('x-hub-signature') || '';
  if (!(await verifyHmac(raw, signature, env.WEBHOOK_SECRET))) {
    return new Response('Invalid signature', { status: 401 });
  }

  let feed: YtFeed | undefined;
  try {
    feed = (parser.parse(raw) as { feed?: YtFeed }).feed;
  } catch {
    return new Response('ignored', { status: 200 })
  }

  // Respond fast; finish the work in the background
  waitUntil(processYoutubeHook(feed as YtFeed))
  return new Response('OK', { status: 200 });
}

export default {
  async fetch(req, env, ctx): Promise<Response> {
    if (req.method === 'GET') {
      return challenge(req);
    } else if (req.method === 'POST') {
      return post(req);
    } else {
      return new Response('Invalid signature', { status: 401 });
    }
  },
  async scheduled(controller, env, ctx) {
    ctx.waitUntil(registerAll());
  },
  async queue(batch: MessageBatch<YoutubeMessageBody>, env: Env, ctx: ExecutionContext): Promise<void> {
    for (const msg of batch.messages) {
      try {
        switch (msg.body.action) {
          case "register_profile":
            await registerYoutubeChannel(msg.body.profile);
            break;
        }
        msg.ack();
      } catch (e) {
        console.error(e);
        msg.retry();
      }
    }
  }
} satisfies ExportedHandler<Env, YoutubeMessageBody>;
