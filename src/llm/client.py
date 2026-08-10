from dataclasses import dataclass
import json
import re
from time import perf_counter

from anthropic import Anthropic
from openai import OpenAI
from pydantic import BaseModel, Field, ValidationError

from src.config.settings import get_settings
from src.models.schemas import QueryPlanDraft
from src.observability.llm_observer import LLMCallObserver


@dataclass
class GeneratedSQL:
    sql: str
    explanation: str
    accessed_tables: list[str]
    accessed_columns: list[str]
    model_confidence: float
    token_usage: dict[str, int | str]


@dataclass
class GeneratedQueryPlan:
    plan: QueryPlanDraft
    confidence: float
    token_usage: dict[str, int | str]


class StructuredSQLResponse(BaseModel):
    sql: str = Field(min_length=1)
    explanation: str = Field(min_length=1)
    confidence: float = Field(ge=0.0, le=1.0)
    tables_accessed: list[str] = Field(default_factory=list)
    columns_accessed: list[str] = Field(default_factory=list)


class StructuredQueryPlanResponse(BaseModel):
    intent: str = Field(default="select")
    target_tables: list[str] = Field(default_factory=list)
    target_columns: list[str] = Field(default_factory=list)
    grouping: list[str] = Field(default_factory=list)
    aggregations: list[str] = Field(default_factory=list)
    filters: list[str] = Field(default_factory=list)
    join_path: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)


def _fixture(
    sql: str,
    explanation: str,
    tables: list[str],
    columns: list[str],
    back_translation: str,
) -> dict:
    return {
        "sql": sql,
        "explanation": explanation,
        "tables_accessed": tables,
        "columns_accessed": columns,
        "back_translation": back_translation,
    }


BENCHMARK_SQL_FIXTURES: dict[str, dict] = {
    "list employees in engineering": _fixture(
        "SELECT e.first_name, e.last_name, d.name AS department_name FROM employees e JOIN departments d ON e.department_id = d.id WHERE d.name = 'Engineering'",
        "Filter employees by Engineering department.",
        ["employees", "departments"],
        ["employees.first_name", "employees.last_name", "departments.name"],
        "List employees in Engineering",
    ),
    "show sales in north america": _fixture(
        "SELECT amount, sale_date, region, channel FROM sales WHERE region = 'North America'",
        "Filter sales by region.",
        ["sales"],
        ["sales.amount", "sales.sale_date", "sales.region", "sales.channel"],
        "Show sales in North America",
    ),
    "list departments": _fixture(
        "SELECT name, cost_center FROM departments",
        "List departments.",
        ["departments"],
        ["departments.name", "departments.cost_center"],
        "List departments",
    ),
    "show employees hired after 2021-01-01": _fixture(
        "SELECT first_name, last_name, hired_at FROM employees WHERE hired_at > '2021-01-01'",
        "Filter employees by hire date.",
        ["employees"],
        ["employees.first_name", "employees.last_name", "employees.hired_at"],
        "Show employees hired after 2021-01-01",
    ),
    "show products with price over 100": _fixture(
        "SELECT name, price FROM products WHERE price > 100",
        "Filter products by price.",
        ["products"],
        ["products.name", "products.price"],
        "Show products with price over 100",
    ),
    "show customers from california": _fixture(
        "SELECT name, state FROM customers WHERE state = 'CA'",
        "Filter customers by state.",
        ["customers"],
        ["customers.name", "customers.state"],
        "Show customers from California",
    ),
    "list paid invoices": _fixture(
        "SELECT status, total FROM invoices WHERE status = 'paid'",
        "Filter paid invoices.",
        ["invoices"],
        ["invoices.status", "invoices.total"],
        "List paid invoices",
    ),
    "show orders from 2026": _fixture(
        "SELECT id, order_date, status, total FROM orders WHERE order_date >= '2026-01-01' AND order_date < '2027-01-01'",
        "Filter orders by year.",
        ["orders"],
        ["orders.order_date"],
        "Show orders from 2026",
    ),
    "list payments above 500": _fixture(
        "SELECT amount, paid_at, method FROM payments WHERE amount > 500",
        "Filter payments by amount.",
        ["payments"],
        ["payments.amount"],
        "List payments above 500",
    ),
    "show direct channel sales": _fixture(
        "SELECT amount, sale_date, region, channel FROM sales WHERE channel = 'Direct'",
        "Filter sales by channel.",
        ["sales"],
        ["sales.channel", "sales.amount"],
        "Show direct channel sales",
    ),
    "show total revenue by region": _fixture(
        "SELECT region, SUM(amount) AS total_revenue FROM sales GROUP BY region",
        "Aggregate sales revenue by region.",
        ["sales"],
        ["sales.region", "sales.amount"],
        "Show total revenue by region",
    ),
    "count employees by department": _fixture(
        "SELECT d.name, COUNT(e.id) AS employee_count FROM departments d LEFT JOIN employees e ON e.department_id = d.id GROUP BY d.name",
        "Count employees by department.",
        ["departments", "employees"],
        ["departments.name", "employees.id"],
        "Count employees by department",
    ),
    "average order value by customer": _fixture(
        "SELECT c.name, AVG(o.total) AS average_order_value FROM customers c JOIN orders o ON o.customer_id = c.id GROUP BY c.name",
        "Average order value by customer.",
        ["orders", "customers"],
        ["orders.total", "customers.name"],
        "Average order value by customer",
    ),
    "top five products by sales quantity": _fixture(
        "SELECT p.name, SUM(oi.quantity) AS total_quantity FROM products p JOIN order_items oi ON oi.product_id = p.id GROUP BY p.name ORDER BY total_quantity DESC LIMIT 5",
        "Rank products by sales quantity.",
        ["products", "order_items"],
        ["products.name", "order_items.quantity"],
        "Top five products by sales quantity",
    ),
    "total invoice value by status": _fixture(
        "SELECT status, SUM(total) AS total_invoice_value FROM invoices GROUP BY status",
        "Aggregate invoice value by status.",
        ["invoices"],
        ["invoices.total", "invoices.status"],
        "Total invoice value by status",
    ),
    "count customers by state": _fixture(
        "SELECT state, COUNT(id) AS customer_count FROM customers GROUP BY state",
        "Count customers by state.",
        ["customers"],
        ["customers.id", "customers.state"],
        "Count customers by state",
    ),
    "show monthly sales revenue": _fixture(
        "SELECT DATE_TRUNC('month', sale_date) AS sale_month, SUM(amount) AS monthly_revenue FROM sales GROUP BY sale_month ORDER BY sale_month",
        "Aggregate sales revenue by month.",
        ["sales"],
        ["sales.sale_date", "sales.amount"],
        "Show monthly sales revenue",
    ),
    "average salary by department": _fixture(
        "SELECT d.name, AVG(e.salary) AS average_salary FROM departments d JOIN employees e ON e.department_id = d.id GROUP BY d.name",
        "Average salary by department.",
        ["departments", "employees"],
        ["employees.salary", "departments.name"],
        "Average salary by department",
    ),
    "total quantity sold by product": _fixture(
        "SELECT p.name, SUM(oi.quantity) AS total_quantity FROM products p JOIN order_items oi ON oi.product_id = p.id GROUP BY p.name",
        "Aggregate quantity sold by product.",
        ["products", "order_items"],
        ["products.name", "order_items.quantity"],
        "Total quantity sold by product",
    ),
    "count orders by status": _fixture(
        "SELECT status, COUNT(id) AS order_count FROM orders GROUP BY status",
        "Count orders by status.",
        ["orders"],
        ["orders.status"],
        "Count orders by status",
    ),
    "show employee names and department names": _fixture(
        "SELECT e.first_name, e.last_name, d.name AS department_name FROM employees e JOIN departments d ON e.department_id = d.id",
        "Join employee names with department names.",
        ["employees", "departments"],
        ["employees.first_name", "employees.last_name", "departments.name"],
        "Show employee names and department names",
    ),
    "list customers with their orders": _fixture(
        "SELECT c.name, o.id AS order_id, o.status FROM customers c JOIN orders o ON o.customer_id = c.id",
        "Join customers with orders.",
        ["customers", "orders"],
        ["customers.name", "orders.id"],
        "List customers with their orders",
    ),
    "show order items with product names": _fixture(
        "SELECT oi.quantity, p.name AS product_name FROM order_items oi JOIN products p ON p.id = oi.product_id",
        "Join order items with products.",
        ["order_items", "products"],
        ["order_items.quantity", "products.name"],
        "Show order items with product names",
    ),
    "list invoices with customer names": _fixture(
        "SELECT i.id, c.name AS customer_name, i.total FROM invoices i JOIN customers c ON c.id = i.customer_id",
        "Join invoices with customers.",
        ["invoices", "customers"],
        ["invoices.id", "customers.name"],
        "List invoices with customer names",
    ),
    "show payments with invoice numbers": _fixture(
        "SELECT p.amount, i.id AS invoice_id FROM payments p JOIN invoices i ON i.id = p.invoice_id",
        "Join payments with invoices.",
        ["payments", "invoices"],
        ["payments.amount", "invoices.id"],
        "Show payments with invoice numbers",
    ),
    "list orders with shipping addresses": _fixture(
        "SELECT o.shipping_address, c.name AS customer_name FROM orders o JOIN customers c ON c.id = o.customer_id",
        "Join orders with customers for shipping addresses.",
        ["orders", "customers"],
        ["orders.shipping_address", "customers.name"],
        "List orders with shipping addresses",
    ),
    "show sales rep names with sales amounts": _fixture(
        "SELECT e.first_name, e.last_name, s.amount FROM sales s JOIN employees e ON e.id = s.employee_id",
        "Join sales with employee names.",
        ["employees", "sales"],
        ["employees.first_name", "employees.last_name", "sales.amount"],
        "Show sales rep names with sales amounts",
    ),
    "show order totals by customer state": _fixture(
        "SELECT c.state, SUM(o.total) AS order_total FROM orders o JOIN customers c ON c.id = o.customer_id GROUP BY c.state",
        "Aggregate order totals by customer state.",
        ["orders", "customers"],
        ["orders.total", "customers.state"],
        "Show order totals by customer state",
    ),
    "list products bought by acme corp": _fixture(
        "SELECT p.name FROM products p JOIN order_items oi ON oi.product_id = p.id JOIN orders o ON o.id = oi.order_id JOIN customers c ON c.id = o.customer_id WHERE c.name = 'Acme Corp'",
        "Find products bought by Acme Corp.",
        ["products", "order_items", "orders", "customers"],
        ["products.name", "customers.name"],
        "List products bought by Acme Corp",
    ),
    "show department payroll and sales totals": _fixture(
        "SELECT d.name, SUM(e.salary) AS payroll_total, SUM(s.amount) AS sales_total FROM departments d JOIN employees e ON e.department_id = d.id LEFT JOIN sales s ON s.employee_id = e.id GROUP BY d.name",
        "Aggregate payroll and sales totals by department.",
        ["departments", "employees", "sales"],
        ["departments.name", "employees.salary", "sales.amount"],
        "Show department payroll and sales totals",
    ),
}


class LLMClient:
    """Provider abstraction for OpenAI/Anthropic text-to-SQL generation."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self.observer: LLMCallObserver | None = None

    def _system_prompt(self) -> str:
        return (
            "You are a Text-to-SQL assistant. Return strictly read-only SQL. "
            "Never return DDL or DML statements."
        )

    def _user_prompt(self, question: str, prompt_context: str) -> str:
        return (
            f"Question: {question}\n\n"
            f"{prompt_context}\n\n"
            "If the schema cannot answer the question, set sql to UNANSWERABLE.\n"
            "If the question is ambiguous, set sql to UNANSWERABLE and explain ambiguity.\n"
            "Return valid JSON with keys: "
            "sql, explanation, confidence, tables_accessed, columns_accessed."
        )

    def _plan_prompt(self, question: str, prompt_context: str) -> str:
        return (
            f"Question: {question}\n\n"
            f"{prompt_context}\n\n"
            "Create a grounded query plan before SQL generation.\n"
            "Use only schema-linked tables/columns. Never invent metrics or join paths.\n"
            "If the question is not answerable, leave target_tables/target_columns empty and explain in notes.\n"
            "Return valid JSON with keys: intent, target_tables, target_columns, grouping, aggregations, "
            "filters, join_path, notes, confidence."
        )

    def _json_chat_openai(self, system: str, user: str, operation: str) -> dict:
        started_at = perf_counter()
        model = self.settings.llm_model or "gpt-4o-mini"
        if self.observer:
            self.observer.on_attempt(operation, "openai", model)
        client = OpenAI(api_key=self.settings.openai_api_key)
        try:
            response = client.chat.completions.create(
                model=model,
                temperature=0,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
            )
        except Exception as exc:
            if self.observer:
                self.observer.on_failure(
                    operation, type(exc).__name__, int((perf_counter() - started_at) * 1000)
                )
            raise
        raw = response.choices[0].message.content or "{}"
        parsed = json.loads(raw)
        usage = getattr(response, "usage", None)
        parsed["_meta"] = {
            "provider": "openai",
            "model": model,
            "prompt_tokens": int(getattr(usage, "prompt_tokens", 0) or 0),
            "completion_tokens": int(getattr(usage, "completion_tokens", 0) or 0),
            "total_tokens": int(getattr(usage, "total_tokens", 0) or 0),
        }
        if self.observer:
            self.observer.on_success(
                operation, parsed["_meta"], int((perf_counter() - started_at) * 1000)
            )
        return parsed

    def _json_chat_anthropic(self, system: str, user: str, operation: str) -> dict:
        started_at = perf_counter()
        model = self.settings.llm_model or "claude-3-5-sonnet-latest"
        if self.observer:
            self.observer.on_attempt(operation, "anthropic", model)
        client = Anthropic(api_key=self.settings.anthropic_api_key)
        try:
            message = client.messages.create(
                model=model,
                max_tokens=1000,
                temperature=0,
                system=system,
                messages=[{"role": "user", "content": user}],
            )
        except Exception as exc:
            if self.observer:
                self.observer.on_failure(
                    operation, type(exc).__name__, int((perf_counter() - started_at) * 1000)
                )
            raise
        parts = [part.text for part in message.content if getattr(part, "type", "") == "text"]
        raw = "".join(parts).strip() or "{}"
        parsed = json.loads(raw)
        usage = getattr(message, "usage", None)
        input_tokens = int(getattr(usage, "input_tokens", 0) or 0)
        output_tokens = int(getattr(usage, "output_tokens", 0) or 0)
        parsed["_meta"] = {
            "provider": "anthropic",
            "model": model,
            "prompt_tokens": input_tokens,
            "completion_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
        }
        if self.observer:
            self.observer.on_success(
                operation, parsed["_meta"], int((perf_counter() - started_at) * 1000)
            )
        return parsed

    def _text_chat_openai(self, system: str, user: str, operation: str) -> str:
        started_at = perf_counter()
        model = self.settings.llm_model or "gpt-4o-mini"
        if self.observer:
            self.observer.on_attempt(operation, "openai", model)
        client = OpenAI(api_key=self.settings.openai_api_key)
        try:
            response = client.chat.completions.create(
                model=model,
                temperature=0,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
            )
        except Exception as exc:
            if self.observer:
                self.observer.on_failure(
                    operation, type(exc).__name__, int((perf_counter() - started_at) * 1000)
                )
            raise
        usage = getattr(response, "usage", None)
        if self.observer:
            self.observer.on_success(
                operation,
                {
                    "provider": "openai",
                    "model": model,
                    "prompt_tokens": int(getattr(usage, "prompt_tokens", 0) or 0),
                    "completion_tokens": int(getattr(usage, "completion_tokens", 0) or 0),
                    "total_tokens": int(getattr(usage, "total_tokens", 0) or 0),
                },
                int((perf_counter() - started_at) * 1000),
            )
        return (response.choices[0].message.content or "").strip()

    def _text_chat_anthropic(self, system: str, user: str, operation: str) -> str:
        started_at = perf_counter()
        model = self.settings.llm_model or "claude-3-5-sonnet-latest"
        if self.observer:
            self.observer.on_attempt(operation, "anthropic", model)
        client = Anthropic(api_key=self.settings.anthropic_api_key)
        try:
            message = client.messages.create(
                model=model,
                max_tokens=600,
                temperature=0,
                system=system,
                messages=[{"role": "user", "content": user}],
            )
        except Exception as exc:
            if self.observer:
                self.observer.on_failure(
                    operation, type(exc).__name__, int((perf_counter() - started_at) * 1000)
                )
            raise
        parts = [part.text for part in message.content if getattr(part, "type", "") == "text"]
        usage = getattr(message, "usage", None)
        input_tokens = int(getattr(usage, "input_tokens", 0) or 0)
        output_tokens = int(getattr(usage, "output_tokens", 0) or 0)
        if self.observer:
            self.observer.on_success(
                operation,
                {
                    "provider": "anthropic",
                    "model": model,
                    "prompt_tokens": input_tokens,
                    "completion_tokens": output_tokens,
                    "total_tokens": input_tokens + output_tokens,
                },
                int((perf_counter() - started_at) * 1000),
            )
        return "".join(parts).strip()

    def _placeholder(self) -> StructuredSQLResponse:
        return StructuredSQLResponse(
            sql="SELECT 'Text-to-SQL scaffold ready' AS status",
            explanation=(
                "Placeholder SQL returned by scaffold. "
                "Configure provider keys to enable live generation."
            ),
            confidence=0.45,
            tables_accessed=[],
            columns_accessed=[],
        )

    def _provider(self) -> str:
        return (self.settings.llm_provider or "").strip().lower()

    def _is_openai_enabled(self) -> bool:
        return self._provider() == "openai" and bool(self.settings.openai_api_key)

    def _is_anthropic_enabled(self) -> bool:
        return self._provider() == "anthropic" and bool(self.settings.anthropic_api_key)

    def _is_deterministic_enabled(self) -> bool:
        return self._provider() in {"deterministic", "fake", "fixture"}

    def _deterministic_sql_payload(self, question: str) -> dict:
        q = question.lower()
        sql = "UNANSWERABLE"
        tables: list[str] = []
        columns: list[str] = []
        explanation = "Deterministic fixture could not map the question to the benchmark schema."
        if "drop " in q or "delete " in q or "update " in q or "insert " in q or "truncate " in q:
            sql = "DROP TABLE employees"
            explanation = "Unsafe deterministic fixture used to exercise guardrails."
        elif q in BENCHMARK_SQL_FIXTURES:
            fixture = BENCHMARK_SQL_FIXTURES[q]
            sql = fixture["sql"]
            tables = fixture["tables_accessed"]
            columns = fixture["columns_accessed"]
            explanation = fixture["explanation"]
        elif "department" in q and "employee" in q:
            sql = (
                "SELECT e.first_name, e.last_name, d.name AS department_name "
                "FROM employees e JOIN departments d ON e.department_id = d.id"
            )
            tables = ["employees", "departments"]
            columns = ["employees.first_name", "employees.last_name", "departments.name"]
            explanation = "Join employees to departments."
        elif "employees in engineering" in q:
            sql = (
                "SELECT e.first_name, e.last_name FROM employees e "
                "JOIN departments d ON e.department_id = d.id WHERE d.name = 'Engineering'"
            )
            tables = ["employees", "departments"]
            columns = ["employees.first_name", "employees.last_name", "departments.name"]
            explanation = "Filter employees by Engineering department."
        elif "hired after" in q:
            sql = "SELECT first_name, last_name, hired_at FROM employees WHERE hired_at > '2021-01-01'"
            tables = ["employees"]
            columns = ["employees.first_name", "employees.last_name", "employees.hired_at"]
            explanation = "Filter employees by hire date."
        elif "average salary" in q:
            sql = (
                "SELECT d.name, AVG(e.salary) AS average_salary FROM departments d "
                "JOIN employees e ON e.department_id = d.id GROUP BY d.name"
            )
            tables = ["departments", "employees"]
            columns = ["departments.name", "employees.salary"]
            explanation = "Average salary by department."
        elif "count employees" in q:
            sql = (
                "SELECT d.name, COUNT(e.id) AS employee_count FROM departments d "
                "LEFT JOIN employees e ON e.department_id = d.id GROUP BY d.name"
            )
            tables = ["departments", "employees"]
            columns = ["departments.name", "employees.id"]
            explanation = "Count employees by department."
        elif "total revenue" in q or "total sales" in q:
            sql = "SELECT region, SUM(amount) AS total_revenue FROM sales GROUP BY region"
            tables = ["sales"]
            columns = ["sales.region", "sales.amount"]
            explanation = "Aggregate sales amount by region."
        elif "sales in" in q or "sales above" in q:
            sql = "SELECT amount, sale_date, region, channel FROM sales WHERE amount > 0"
            tables = ["sales"]
            columns = ["sales.amount", "sales.sale_date", "sales.region", "sales.channel"]
            explanation = "List sales rows."
        elif "all departments" in q or "list departments" in q:
            sql = "SELECT name, cost_center FROM departments"
            tables = ["departments"]
            columns = ["departments.name", "departments.cost_center"]
            explanation = "List departments."
        elif "all employees" in q or "show employees" in q:
            sql = "SELECT first_name, last_name, title FROM employees"
            tables = ["employees"]
            columns = ["employees.first_name", "employees.last_name", "employees.title"]
            explanation = "List employees."
        return {
            "sql": sql,
            "explanation": explanation,
            "confidence": 0.9 if sql != "UNANSWERABLE" else 0.1,
            "tables_accessed": tables,
            "columns_accessed": columns,
            "_meta": {
                "provider": "deterministic",
                "model": "sqlgenx-fixture-v1",
                "prompt_tokens": 20,
                "completion_tokens": 20,
                "total_tokens": 40,
            },
        }

    def _deterministic_json(self, operation: str, question: str) -> dict:
        started_at = perf_counter()
        meta = {
            "provider": "deterministic",
            "model": "sqlgenx-fixture-v1",
            "prompt_tokens": 20,
            "completion_tokens": 20,
            "total_tokens": 40,
        }
        if self.observer:
            self.observer.on_attempt(operation, meta["provider"], meta["model"])
        parsed = self._deterministic_sql_payload(question)
        if operation == "query_plan_generation_ms":
            payload = {
                "intent": "select",
                "target_tables": parsed.get("tables_accessed", []),
                "target_columns": parsed.get("columns_accessed", []),
                "grouping": [],
                "aggregations": ["SUM"] if "SUM(" in parsed.get("sql", "") else [],
                "filters": [],
                "join_path": parsed.get("tables_accessed", []),
                "notes": ["Deterministic fixture plan."],
                "confidence": parsed.get("confidence", 0.5),
                "_meta": meta,
            }
        else:
            payload = parsed
            payload["_meta"] = meta
        if self.observer:
            self.observer.on_success(operation, meta, int((perf_counter() - started_at) * 1000))
        return payload

    def generate_structured_sql(self, question: str, prompt_context: str) -> GeneratedSQL:
        response: StructuredSQLResponse
        meta: dict[str, int | str] = {
            "provider": self._provider() or "none",
            "model": self.settings.llm_model or "",
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        }
        try:
            if self._is_deterministic_enabled():
                parsed = self._deterministic_json("primary_sql_generation_ms", question)
                meta = parsed.pop("_meta", meta)
                response = StructuredSQLResponse.model_validate(parsed)
            elif self._is_openai_enabled():
                parsed = self._json_chat_openai(
                    self._system_prompt(),
                    self._user_prompt(question, prompt_context),
                    "primary_sql_generation_ms",
                )
                meta = parsed.pop("_meta", meta)
                response = StructuredSQLResponse.model_validate(parsed)
            elif self._is_anthropic_enabled():
                parsed = self._json_chat_anthropic(
                    self._system_prompt(),
                    self._user_prompt(question, prompt_context),
                    "primary_sql_generation_ms",
                )
                meta = parsed.pop("_meta", meta)
                response = StructuredSQLResponse.model_validate(parsed)
            else:
                response = self._placeholder()
        except (ValidationError, json.JSONDecodeError, Exception):
            response = self._placeholder()

        return GeneratedSQL(
            sql=response.sql.strip(),
            explanation=response.explanation,
            accessed_tables=response.tables_accessed,
            accessed_columns=response.columns_accessed,
            model_confidence=response.confidence,
            token_usage=meta,
        )

    def generate_query_plan(self, question: str, prompt_context: str) -> GeneratedQueryPlan:
        response: StructuredQueryPlanResponse
        meta: dict[str, int | str] = {
            "provider": self._provider() or "none",
            "model": self.settings.llm_model or "",
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        }
        try:
            if self._is_deterministic_enabled():
                parsed = self._deterministic_json("query_plan_generation_ms", question)
                meta = parsed.pop("_meta", meta)
                response = StructuredQueryPlanResponse.model_validate(parsed)
            elif self._is_openai_enabled():
                parsed = self._json_chat_openai(
                    self._system_prompt(),
                    self._plan_prompt(question, prompt_context),
                    "query_plan_generation_ms",
                )
                meta = parsed.pop("_meta", meta)
                response = StructuredQueryPlanResponse.model_validate(parsed)
            elif self._is_anthropic_enabled():
                parsed = self._json_chat_anthropic(
                    self._system_prompt(),
                    self._plan_prompt(question, prompt_context),
                    "query_plan_generation_ms",
                )
                meta = parsed.pop("_meta", meta)
                response = StructuredQueryPlanResponse.model_validate(parsed)
            else:
                response = StructuredQueryPlanResponse()
        except (ValidationError, json.JSONDecodeError, Exception):
            response = StructuredQueryPlanResponse()

        return GeneratedQueryPlan(
            plan=QueryPlanDraft(
                intent=response.intent,
                target_tables=response.target_tables,
                target_columns=response.target_columns,
                grouping=response.grouping,
                aggregations=response.aggregations,
                filters=response.filters,
                join_path=response.join_path,
                notes=response.notes,
            ),
            confidence=response.confidence,
            token_usage=meta,
        )

    def back_translate_sql(self, sql: str, prompt_context: str = "") -> str:
        system = "You explain SQL in plain English question form."
        user = (
            "Given this SQL, write the exact user question it answers in one sentence.\n\n"
            f"SQL:\n{sql}\n\n"
            f"Schema context:\n{prompt_context}\n"
        )
        try:
            if self._is_deterministic_enabled():
                if self.observer:
                    meta = {
                        "provider": "deterministic",
                        "model": "sqlgenx-fixture-v1",
                        "prompt_tokens": 10,
                        "completion_tokens": 10,
                        "total_tokens": 20,
                    }
                    self.observer.on_attempt("alignment_validation_ms", meta["provider"], meta["model"])
                    self.observer.on_success("alignment_validation_ms", meta, 0)
                return self._heuristic_back_translation(sql)
            if self._is_openai_enabled():
                text = self._text_chat_openai(system, user, "alignment_validation_ms")
                if text:
                    return text
            if self._is_anthropic_enabled():
                text = self._text_chat_anthropic(system, user, "alignment_validation_ms")
                if text:
                    return text
        except Exception:
            pass
        return self._heuristic_back_translation(sql)

    def generate_alternative_sql(
        self, question: str, prompt_context: str, primary_sql: str
    ) -> GeneratedSQL:
        variation_prompt = (
            f"{prompt_context}\n\n"
            f"Original user question: {question}\n"
            f"Primary SQL approach:\n{primary_sql}\n\n"
            "Generate an alternative SQL approach that answers the same question, "
            "ideally with different join/aggregation strategy when possible. "
            "Return JSON with keys: sql, explanation, confidence, tables_accessed, columns_accessed."
        )
        try:
            if self._is_deterministic_enabled():
                parsed = self._deterministic_json("alternative_sql_generation_ms", question)
                meta = parsed.pop("_meta", {})
                response = StructuredSQLResponse.model_validate(parsed)
                return GeneratedSQL(
                    sql=response.sql.strip(),
                    explanation=response.explanation,
                    accessed_tables=response.tables_accessed,
                    accessed_columns=response.columns_accessed,
                    model_confidence=response.confidence,
                    token_usage=meta,
                )
            if self._is_openai_enabled():
                parsed = self._json_chat_openai(
                    self._system_prompt(), variation_prompt, "alternative_sql_generation_ms"
                )
                meta = parsed.pop(
                    "_meta",
                    {
                        "provider": "openai",
                        "model": self.settings.llm_model or "gpt-4o-mini",
                        "prompt_tokens": 0,
                        "completion_tokens": 0,
                        "total_tokens": 0,
                    },
                )
                response = StructuredSQLResponse.model_validate(parsed)
                return GeneratedSQL(
                    sql=response.sql.strip(),
                    explanation=response.explanation,
                    accessed_tables=response.tables_accessed,
                    accessed_columns=response.columns_accessed,
                    model_confidence=response.confidence,
                    token_usage=meta,
                )
            if self._is_anthropic_enabled():
                parsed = self._json_chat_anthropic(
                    self._system_prompt(), variation_prompt, "alternative_sql_generation_ms"
                )
                meta = parsed.pop(
                    "_meta",
                    {
                        "provider": "anthropic",
                        "model": self.settings.llm_model or "claude-3-5-sonnet-latest",
                        "prompt_tokens": 0,
                        "completion_tokens": 0,
                        "total_tokens": 0,
                    },
                )
                response = StructuredSQLResponse.model_validate(parsed)
                return GeneratedSQL(
                    sql=response.sql.strip(),
                    explanation=response.explanation,
                    accessed_tables=response.tables_accessed,
                    accessed_columns=response.columns_accessed,
                    model_confidence=response.confidence,
                    token_usage=meta,
                )
        except (ValidationError, json.JSONDecodeError, Exception):
            pass
        return GeneratedSQL(
            sql=primary_sql,
            explanation="Fallback alternative SQL reuses primary query.",
            accessed_tables=[],
            accessed_columns=[],
            model_confidence=0.4,
            token_usage={
                "provider": self._provider() or "none",
                "model": self.settings.llm_model or "",
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
            },
        )

    def _heuristic_back_translation(self, sql: str) -> str:
        normalized = " ".join(sql.strip().split())
        for fixture in BENCHMARK_SQL_FIXTURES.values():
            if normalized.lower() == " ".join(fixture["sql"].split()).lower():
                return fixture["back_translation"]
        table_match = re.search(r"\bFROM\s+([a-zA-Z_][\w\.]*)", normalized, flags=re.IGNORECASE)
        group_match = re.search(r"\bGROUP\s+BY\b", normalized, flags=re.IGNORECASE)
        where_match = re.search(r"\bWHERE\b", normalized, flags=re.IGNORECASE)
        table = table_match.group(1) if table_match else "the dataset"
        if group_match and where_match:
            return f"What aggregated metrics from {table} satisfy the query filters?"
        if group_match:
            return f"What aggregated metrics are grouped from {table}?"
        if where_match:
            return f"What rows from {table} satisfy the filters?"
        return f"What records are selected from {table}?"
