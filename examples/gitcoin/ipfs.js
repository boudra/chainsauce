import fetch from "node-fetch";

function wait(delay) {
  return new Promise((resolve) => setTimeout(resolve, delay));
}

export async function fetchJson(cid, retries = 5) {
  let attempt = 0;

  while (attempt < retries) {
    try {
      const res = await fetch(`https://cloudflare-ipfs.com/ipfs/${cid}`);
      return await res.json();
    } catch (e) {
      attempt = attempt + 1;
      await wait(attempt * 500);
      console.log("[IPFS] Retrying:", cid, "Attempt:", attempt);
    }
  }
}
