import { test, expect } from "@playwright/test";

test("loads NL to SQL assistant UI", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByText("Enterprise NL to SQL Assistant")).toBeVisible();
  await expect(page.getByRole("button", { name: "Generate SQL" })).toBeVisible();
});
