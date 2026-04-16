import { createRemoteJWKSet, jwtVerify, type JWTPayload } from "jose";

let jwksCache: ReturnType<typeof createRemoteJWKSet> | null = null;

function getIssuer(): string {
  const region = process.env.COGNITO_REGION;
  const poolId = process.env.COGNITO_USER_POOL_ID;
  if (!region || !poolId) {
    throw new Error("Missing Cognito config (COGNITO_REGION, COGNITO_USER_POOL_ID).");
  }
  return `https://cognito-idp.${region}.amazonaws.com/${poolId}`;
}

function getJwks() {
  if (!jwksCache) {
    const jwksUrl = `${getIssuer()}/.well-known/jwks.json`;
    jwksCache = createRemoteJWKSet(new URL(jwksUrl));
  }
  return jwksCache;
}

export async function verifyBearerToken(token: string): Promise<JWTPayload> {
  const issuer = getIssuer();
  const clientId = process.env.COGNITO_CLIENT_ID;
  if (!clientId) {
    throw new Error("Missing COGNITO_CLIENT_ID.");
  }

  const { payload } = await jwtVerify(token, getJwks(), {
    issuer,
    audience: clientId
  });

  return payload;
}
