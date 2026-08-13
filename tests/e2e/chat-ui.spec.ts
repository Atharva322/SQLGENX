import { test, expect } from "@playwright/test";

test("loads NL to SQL assistant UI", async ({ page }) => {
  await page.route("**/api/adapters", async (route) => {
    await route.fulfill({
      json: {
        adapters: [
          {
            key: "mysql",
            displayName: "MySQL / MariaDB",
            releaseState: "verified",
            capabilities: { readOnlyExecution: true, schemaIntrospection: true }
          },
          {
            key: "postgresql",
            displayName: "PostgreSQL",
            releaseState: "verified",
            capabilities: { readOnlyExecution: true, schemaIntrospection: true }
          }
        ]
      }
    });
  });
  await page.route("**/api/connections", async (route) => {
    if (route.request().method() === "GET") {
      await route.fulfill({
        json: {
          connections: [
            {
              id: "default",
              displayName: "Default",
              adapterKey: "postgresql",
              dialect: "postgres",
              verificationState: "legacy_env",
              healthState: "unknown",
              version: 1,
              createdAt: "2026-08-12T00:00:00Z",
              updatedAt: "2026-08-12T00:00:00Z"
            }
          ]
        }
      });
      return;
    }
    const payload = route.request().postDataJSON() as { adapterKey?: string; id?: string; displayName?: string };
    await route.fulfill({
      json: {
        id: payload.id ?? "runtime_mysql",
        displayName: payload.displayName ?? "Runtime MySQL",
        adapterKey: payload.adapterKey ?? "mysql",
        dialect: payload.adapterKey === "mysql" ? "mysql" : "postgres",
        verificationState: "verified",
        healthState: "healthy",
        version: 1,
        createdAt: "2026-08-12T00:00:00Z",
        updatedAt: "2026-08-12T00:00:00Z"
      }
    });
  });
  await page.route("**/api/connections/test", async (route) => {
    await route.fulfill({ json: { ok: true, schemaFingerprint: "fp-runtime" } });
  });
  await page.route("**/api/connections/runtime_pg/schema/refresh", async (route) => {
    await route.fulfill({ json: { database: "runtime_pg", refreshedAt: "2026-08-12T00:00:00Z", tables: [] } });
  });
  await page.route("**/api/chat/generate-sql", async (route) => {
    await route.fulfill({
      json: {
        queryId: "qry_demo",
        connectionId: "default",
        conversationId: "conv_demo",
        generatedSql: "SELECT name FROM departments",
        explanation: "List departments.",
        confidence: 0.92,
        referenced: { tables: ["departments"], columns: ["departments.name"] },
        safety: { status: "safe", reasons: [] },
        preExecuted: {
          queryId: "qry_demo",
          status: "success",
          rows: [{ name: "Engineering" }],
          rowCount: 1,
          executionMs: 8
        }
      }
    });
  });

  await page.goto("/");
  await expect(page.getByText("Enterprise NL to SQL Assistant")).toBeVisible();
  await expect(page.getByLabel("Connection")).toBeVisible();
  await expect(page.getByRole("button", { name: "Generate SQL" })).toBeVisible();
  await expect(page.getByText("Add Connection", { exact: true })).toBeVisible();

  await page.getByRole("combobox").nth(1).selectOption("mysql");
  await expect(page.getByPlaceholder("Port")).toHaveValue("3306");
  await page.getByPlaceholder("Connection ID").fill("runtime_mysql");
  await page.getByPlaceholder("Display name").fill("Runtime MySQL");
  await page.getByPlaceholder("Password").fill("secret-value");
  await page.getByRole("button", { name: "Test Connection" }).click();
  await expect(page.getByText("test passed")).toBeVisible();
  await page.getByRole("button", { name: "Save Connection" }).click();

  await page.getByPlaceholder("Example: Show top 10 customers by total order value in the last 90 days.").fill("List departments");
  await page.getByRole("button", { name: "Generate SQL" }).click();
  await expect(page.getByText("Generated SQL", { exact: true })).toBeVisible();
  await expect(page.getByText("Connection: default")).toBeVisible();
  await expect(page.getByText("Engineering")).toBeVisible();
});
