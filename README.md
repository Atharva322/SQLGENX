# GENXSQL

Enterprise NL-to-SQL assistant for MySQL using Next.js + OpenAI, with review-before-run workflow and read-only safety guardrails.

## Features

- Plain-English to SQL generation (`POST /api/chat/generate-sql`)
- Read-only SQL validation (SELECT/CTE only, no DML/DDL, no multi-statement/comments)
- Manual execution approval (`POST /api/chat/execute-sql`)
- Schema introspection and caching from `INFORMATION_SCHEMA`
- Query history/audit trail per conversation
- Chat UI with SQL preview, safety warnings, pagination, and CSV export
- Cognito JWT-based SSO support (plus optional local dev bypass)

## Quick Start

1. Install dependencies:

```bash
npm install
```

2. Copy env template and set values:

```bash
cp .env.example .env.local
```

3. Start dev server:

```bash
npm run dev
```

4. Open `http://localhost:3000`.

## API Endpoints

- `POST /api/chat/generate-sql`
- `POST /api/chat/execute-sql`
- `GET /api/schema/context`
- `POST /api/schema/refresh`
- `GET /api/history/:conversationId`

## Security Notes

- Use a read-only MySQL account in production.
- Keep `AUTH_DEV_BYPASS=false` outside local development.
- Map Cognito groups to roles:
  - `genxsql-admin` -> `admin`
  - `genxsql-viewer` -> `viewer`
  - default -> `analyst`

## Testing

```bash
npm test
```

```bash
npm run test:e2e
```

## AWS Deployment (Suggested)

- Host Next.js on ECS Fargate, App Runner, or Amplify Hosting.
- Store secrets in AWS Secrets Manager.
- Put app behind ALB + WAF.
- Use Amazon Cognito with SAML/OIDC federation to enterprise IdP.
- Point app to private MySQL endpoint with security group restrictions.
