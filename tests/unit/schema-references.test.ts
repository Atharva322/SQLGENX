import { extractReferences } from "@/lib/sql/references";
import type { SchemaContext } from "@/lib/types/contracts";

const schema: SchemaContext = {
  database: "sales",
  refreshedAt: new Date().toISOString(),
  tables: [
    {
      table: "orders",
      columns: [
        { name: "id", type: "int" },
        { name: "customer_id", type: "int" },
        { name: "total_amount", type: "decimal" }
      ]
    },
    {
      table: "customers",
      columns: [
        { name: "id", type: "int" },
        { name: "name", type: "varchar" }
      ]
    }
  ]
};

describe("extractReferences", () => {
  it("extracts tables and columns from generated SQL", () => {
    const refs = extractReferences(
      "SELECT c.name, o.total_amount FROM customers c JOIN orders o ON c.id = o.customer_id",
      schema
    );
    expect(refs.tables).toEqual(expect.arrayContaining(["customers", "orders"]));
    expect(refs.columns).toEqual(expect.arrayContaining(["orders.total_amount", "orders.customer_id", "customers.name"]));
  });
});
